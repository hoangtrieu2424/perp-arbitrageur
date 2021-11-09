import "./init"

import { MaxUint256 } from "@ethersproject/constants"
import { parseBytes32String } from "@ethersproject/strings"
import { Mutex } from "async-mutex"
import Big from "big.js"
import { Wallet } from "ethers"
import FTXRest from "ftx-api-rest"
import { Service } from "typedi"

import { Amm } from "../types/ethers"
import { AmmConfig, ammConfigMap, preflightCheck } from "./configs"
import { ERC20Service } from "./ERC20Service"
import { EthService } from "./EthService"
import { FtxService, PlaceOrderPayload } from "./FtxService"
import { min } from "./functions"
import { Log } from "./Log"
import { AmmProps, PerpService, PnlCalcOption, Position, PositionChangedLog, Side } from "./PerpService"
import { ServerProfile } from "./ServerProfile"
import { EthMetadata, SystemMetadataFactory } from "./SystemMetadataFactory"
import { spread } from "lodash"
import { fileURLToPath } from "url"

@Service()
export class Arbitrageur {
    private readonly log = Log.getLogger(Arbitrageur.name)
    private readonly nonceMutex = new Mutex()
    private readonly perpfiFee = Big(0.001) // default 0.1%
    private readonly arbitrageur: Wallet
    private readonly ftxClient: any

    private nextNonce!: number
    private perpfiBalance = Big(0)
    private ftxAccountValue = Big(0)

    constructor(
        readonly perpService: PerpService,
        readonly erc20Service: ERC20Service,
        readonly ethService: EthService,
        readonly serverProfile: ServerProfile,
        readonly systemMetadataFactory: SystemMetadataFactory,
        readonly ftxService: FtxService,
    ) {
        this.arbitrageur = ethService.privateKeyToWallet(serverProfile.arbitrageurPK)
        this.ftxClient = new FTXRest({
            key: this.serverProfile.ftxApiKey,
            secret: this.serverProfile.ftxApiSecret,
            subaccount: this.serverProfile.ftxSubaccount,
        })
    }

    async start(): Promise<void> {
        this.log.jinfo({
            event: "Start",
            params: {
                arbitrageur: this.arbitrageur.address,
            },
        })

        await this.arbitrage()
    }

    async startInterval(): Promise<void> {
        this.log.jinfo({
            event: "StartInterval",
            params: {
                arbitrageur: this.arbitrageur.address,
            },
        })

        await this.arbitrage()
        setInterval(async () => await this.arbitrage(), 1000 * 10 * 1) // default 1 minute
    }

    async checkBlockFreshness(): Promise<void> {
        const latestBlockNumber = await this.ethService.provider.getBlockNumber()
        const latestBlock = await this.ethService.getBlock(latestBlockNumber)
        const diffNowSeconds = Math.floor(Date.now() / 1000) - latestBlock.timestamp
        this.log.jinfo({
            event: "LatestBlock",
            params: {
                latestBlockNumber,
                diffNowSeconds,
            },
        })
        if (diffNowSeconds > preflightCheck.BLOCK_TIMESTAMP_FRESHNESS_THRESHOLD) {
            throw new Error("Get stale block")
        }
    }

    async arbitrage(): Promise<void> {
        this.nextNonce = await this.arbitrageur.getTransactionCount()
        this.log.jinfo({
            event: "NextNonce",
            params: {
                nextNonce: this.nextNonce,
            },
        })

        await this.checkBlockFreshness()

        // Check xDai balance - needed for gas payments
        const xDaiBalance = await this.ethService.getBalance(this.arbitrageur.address)
        this.log.jinfo({
            event: "xDaiBalance",
            params: { balance: xDaiBalance.toFixed() },
        })
        if (xDaiBalance.lt(preflightCheck.XDAI_BALANCE_THRESHOLD)) {
            this.log.jwarn({
                event: "xDaiNotEnough",
                params: { balance: xDaiBalance.toFixed() },
            })
            return
        }

        // get all Amm info
        const systemMetadata = await this.systemMetadataFactory.fetch()
        const amms = await this.perpService.getAllOpenAmms()

        // maintain margin on all amm
        await Promise.all(
            amms.map(async amm => {
                try {
                    return await this.adjustMarginAmm(amm)
                } catch (e) {
                    this.log.jinfo({
                        event: "AdjustMarginAmmFailed",
                        params: {
                            amm: amm.address,
                            reason: e.toString(),
                            stack: e.stack,
                            response: e.response,
                        },
                    })
                    return
                }
            }),
        )

        // Fetch FTX account info
        const ftxAccountInfo = await this.ftxService.getAccountInfo(this.ftxClient)

        // Check FTX balance (USD)
        const ftxBalance = ftxAccountInfo.freeCollateral
        this.log.jinfo({
            event: "FtxUsdBalance",
            params: { balance: ftxBalance.toFixed() },
        })
        if (ftxBalance.lt(preflightCheck.FTX_USD_BALANCE_THRESHOLD)) {
            this.log.jerror({
                event: "FtxUsdNotEnough",
                params: { balance: ftxBalance.toFixed() },
            })
            return
        }

        // Check FTX margin ratio
        const ftxMarginRatio = ftxAccountInfo.marginFraction
        this.log.jinfo({
            event: "FtxMarginRatio",
            params: { ftxMarginRatio: ftxMarginRatio.toFixed() },
        })
        if (!ftxMarginRatio.eq(0) && ftxMarginRatio.lt(preflightCheck.FTX_MARGIN_RATIO_THRESHOLD)) {
            this.log.jerror({
                event: "FtxMarginRatioTooLow",
                params: { balance: ftxMarginRatio.toFixed() },
            })
            return
        }

        this.ftxAccountValue = ftxAccountInfo.totalAccountValue


        // Check all Perpetual Protocol AMMs
        await Promise.all(
            amms.map(async amm => {
                try {
                    return await this.arbitrageAmm(amm, systemMetadata)
                } catch (e) {
                    this.log.jerror({
                        event: "ArbitrageAmmFailed",
                        params: {
                            reason: e.toString(),
                            stackTrace: e.stack,
                        },
                    })
                    return
                }
            }),
        )

        await this.calculateTotalValue(amms)
    }

    async validateAmm(ammAddr: string): Promise<{ ammPair: string; ammConfig: AmmConfig }> {
        const ammState = await this.perpService.getAmmStates(ammAddr)
        const ammPair = await this.getAmmPair(ammState)
        const ammConfig = ammConfigMap[ammPair]

        if (!ammConfig) {
            // do it asynchronously so it is not blocked by outage
            this.log.jwarn({
                event: "AmmConfigMissing",
                params: {
                    amm: ammAddr,
                    ammPair,
                },
            })
            throw new Error("ValidateAmmFailed: missing config")
        }

        if (!ammConfig.ENABLED) {
            this.log.jinfo({
                event: "DisabledAmm",
                params: {
                    amm: ammAddr,
                    ammConfig,
                    ammPair,
                },
            })
            throw new Error("ValidateAmmFailed: Amm disabled")
        }

        return { ammPair, ammConfig }
    }

    async adjustMarginAmm(amm: Amm) {
        const { ammPair, ammConfig } = await this.validateAmm(amm.address)

        this.log.jinfo({
            event: "AdjustMarginAmm",
            params: {
                amm: amm.address,
                ammConfig,
                ammPair,
            },
        })

        const arbitrageurAddr = this.arbitrageur.address
        const priceFeedKey = parseBytes32String(await amm.priceFeedKey())
        const quoteAssetAddr = await amm.quoteAsset()
        const quoteBalance = await this.erc20Service.balanceOf(quoteAssetAddr, arbitrageurAddr)
        const position = await this.perpService.getPersonalPositionWithFundingPayment(amm.address, arbitrageurAddr)
        // Adjust PERP margin
        //   marginRatio = (margin + unrealizedPnl - fundingPayment) / positionNotional
        //
        //   expectedMarginRatio = (margin + unrealizedPnl - fundingPayment + marginToChange) / positionNotional
        //
        //   expectedMarginRatio * positionNotional = margin + unrealizedPnl - fundingPayment + marginToChange
        //
        //   marginToChange = expectedMarginRatio * positionNotional - (margin - fundingPayment) - unrealizedPnl

        // since we never fully close the position, there could be edge cases when the position is extremely small
        // and the margin calculation could be off due to lack of decimal precision. so we want to ignore tiny positions.
        // Note 6-decimal is chosen because we can fairly assume there would be no meaningful positions below this size.
        if (position.size.abs().gte("0.0")) {
            const marginRatio = await this.perpService.getMarginRatio(amm.address, arbitrageurAddr)
            // note positionNotional is an estimation because the real margin ratio is calculated based on two mark price candidates: SPOT & TWAP.
            // we pick the more "conservative" one (SPOT) here so the margin required tends to fall on the safer side
            const { unrealizedPnl: spotUnrealizedPnl, positionNotional: spotPositionNotional } =
                await this.perpService.getPositionNotionalAndUnrealizedPnl(
                    amm.address,
                    this.arbitrageur.address,
                    PnlCalcOption.SPOT_PRICE,
                )
            const expectedMarginRatio = new Big(1).div(ammConfig.PERPFI_LEVERAGE)
            const marginToChange = expectedMarginRatio
                .mul(spotPositionNotional)
                .sub(position.margin) // funding payment already included
                .sub(spotUnrealizedPnl)
            this.log.jinfo({
                event: "MarginRatioBefore",
                params: {
                    amm: amm.address,
                    ammPair,
                    baseAssetSymbol: priceFeedKey,
                    expectedMarginRatio: +expectedMarginRatio,
                    marginRatio: +marginRatio,
                    margin: +position.margin,
                    spotUnrealizedPnl: +spotUnrealizedPnl,
                    spotPositionNotional: +spotPositionNotional,
                    marginToChange: +marginToChange,
                },
            })

            if (marginRatio.gt(expectedMarginRatio.mul(new Big(1).add(ammConfig.ADJUST_MARGIN_RATIO_THRESHOLD)))) {
                let marginToBeRemoved = marginToChange.mul(-1)
                // cap the reduction by the current (funding payment realized) margin
                if (marginToBeRemoved.gt(position.margin)) {
                    marginToBeRemoved = position.margin
                }
                if (marginToBeRemoved.round(6, 0).gt(Big(0))) {
                    this.log.jinfo({
                        event: "RemoveMargin",
                        params: {
                            ammPair,
                            marginToBeRemoved: +marginToBeRemoved,
                            baseAssetSymbol: priceFeedKey,
                        },
                    })

                    const release = await this.nonceMutex.acquire()
                    let tx
                    try {
                        tx = await this.perpService.removeMargin(this.arbitrageur, amm.address, marginToBeRemoved, {
                            nonce: this.nextNonce,
                            gasPrice: await this.ethService.getSafeGasPrice(),
                        })
                        this.nextNonce++
                    } finally {
                        release()
                    }
                    await tx.wait()
                    this.log.jinfo({
                        event: "MarginRatioAfter",
                        params: {
                            ammPair,
                            marginRatio: (
                                await this.perpService.getMarginRatio(amm.address, arbitrageurAddr)
                            ).toFixed(),
                            baseAssetSymbol: priceFeedKey,
                        },
                    })
                }
            } else if (
                marginRatio.lt(expectedMarginRatio.mul(new Big(1).sub(ammConfig.ADJUST_MARGIN_RATIO_THRESHOLD)))
            ) {
                let marginToBeAdded = marginToChange
                marginToBeAdded = marginToBeAdded.gt(quoteBalance) ? quoteBalance : marginToBeAdded
                if (marginToBeAdded.round(6, 0).gt(Big(0))) {
                    this.log.jinfo({
                        event: "AddMargin",
                        params: {
                            ammPair,
                            marginToBeAdded: marginToBeAdded.toFixed(),
                            baseAssetSymbol: priceFeedKey,
                        },
                    })

                    const release = await this.nonceMutex.acquire()
                    let tx
                    try {
                        tx = await this.perpService.addMargin(this.arbitrageur, amm.address, marginToBeAdded, {
                            nonce: this.nextNonce,
                            gasPrice: await this.ethService.getSafeGasPrice(),
                        })
                        this.nextNonce++
                    } finally {
                        release()
                    }
                    await tx.wait()
                    this.log.jinfo({
                        event: "MarginRatioAfter",
                        params: {
                            ammPair,
                            marginRatio: (
                                await this.perpService.getMarginRatio(amm.address, arbitrageurAddr)
                            ).toFixed(),
                            baseAssetSymbol: priceFeedKey,
                        },
                    })
                }
            }
        }
    }
    async arbitrageAmm(amm: Amm, systemMetadata: EthMetadata): Promise<void> {
        const ammState = await this.perpService.getAmmStates(amm.address)
        const { ammPair, ammConfig } = await this.validateAmm(amm.address)
        this.log.jinfo({
            event: "ArbitrageAmm",
            params: {
                amm: amm.address,
                ammConfig,
                ammPair,
            },
        })

        const arbitrageurAddr = this.arbitrageur.address
        const clearingHouseAddr = systemMetadata.clearingHouseAddr
        const quoteAssetAddr = await amm.quoteAsset()

        // Check Perpetual Protocol balance - quote asset is USDC
        const quoteBalance = await this.erc20Service.balanceOf(quoteAssetAddr, arbitrageurAddr)
        if (quoteBalance.lt(preflightCheck.USDC_BALANCE_THRESHOLD)) {
            this.log.jwarn({
                event: "QuoteAssetNotEnough",
                params: { balance: quoteBalance.toFixed() },
            })
            // NOTE we don't abort prematurely here because we don't know yet which direction
            // the arbitrageur will go. If it's the opposite then it doesn't need more quote asset to execute
        }

        this.perpfiBalance = quoteBalance

        // Make sure the quote asset are approved
        const allowance = await this.erc20Service.allowance(quoteAssetAddr, arbitrageurAddr, clearingHouseAddr)
        const infiniteAllowance = await this.erc20Service.fromScaled(quoteAssetAddr, MaxUint256)
        const allowanceThreshold = infiniteAllowance.div(2)
        if (allowance.lt(allowanceThreshold)) {
            await this.erc20Service.approve(quoteAssetAddr, clearingHouseAddr, infiniteAllowance, this.arbitrageur, {
                gasPrice: await this.ethService.getSafeGasPrice(),
            })
            this.log.jinfo({
                event: "SetMaxAllowance",
                params: {
                    quoteAssetAddr: quoteAssetAddr,
                    owner: this.arbitrageur.address,
                    agent: clearingHouseAddr,
                },
            })
        }

        // List Perpetual Protocol positions
        const [position, unrealizedPnl] = await Promise.all([
            this.perpService.getPersonalPositionWithFundingPayment(amm.address, this.arbitrageur.address),
            this.perpService.getUnrealizedPnl(amm.address, this.arbitrageur.address, PnlCalcOption.SPOT_PRICE),
        ])

        this.log.jinfo({
            event: "PerpFiPosition",
            params: {
                ammPair,
                size: +position.size,
                margin: +position.margin,
                openNotional: +position.openNotional,
            },
        })

        this.log.jinfo({
            event: "PerpFiPnL",
            params: {
                ammPair,
                margin: +position.margin,
                unrealizedPnl: +unrealizedPnl,
                quoteBalance: +quoteBalance,
                accountValue: +position.margin.add(unrealizedPnl).add(quoteBalance),
            },
        })

        // List FTX positions
        const ftxPosition = await this.ftxService.getPosition(this.ftxClient, ammConfig.FTX_MARKET_ID)
        if (ftxPosition) {
            const ftxSizeDiff = ftxPosition.netSize.abs().sub(position.size.abs())
            this.log.jinfo({
                event: "FtxPosition",
                params: {
                    marketId: ftxPosition.future,
                    size: +ftxPosition.netSize,
                    diff: +ftxSizeDiff,
                },
            })

            if (ftxSizeDiff.abs().gte(ammConfig.FTX_MIN_TRADE_SIZE)) {
                const mitigation = FtxService.mitigatePositionSizeDiff(position.size, ftxPosition.netSize)
                this.log.jinfo({
                    event: "MitigateFTXPositionSizeDiff",
                    params: {
                        perpfiPositionSize: position.size,
                        ftxPositionSize: ftxPosition.netSize,
                        size: mitigation.sizeAbs,
                        side: mitigation.side,
                    },
                })
                await this.openFTXPosition(ammConfig.FTX_MARKET_ID, mitigation.sizeAbs, mitigation.side)
            }
        }

        // NOTE If the arbitrageur is already out of balance,
        // we will leave it as is and not do any rebalance work

        // Fetch prices
        const [ammPrice, ftxPrice] = await Promise.all([this.fetchAmmPrice(amm), this.fetchFtxPrice(ammConfig)])

        // Calculate spread
        // NOTE We assume FTX liquidity is always larger than Perpetual Protocol,
        // so we use Perpetual Protocol to calculate the max slippage
      
        const spread = ammPrice.sub(ftxPrice).div(ftxPrice) 
        const amount = Arbitrageur.calcMaxSlippageAmount(
            ammPrice,
            ammConfig.MAX_SLIPPAGE_RATIO,
            ammState.baseAssetReserve,
            ammState.quoteAssetReserve,
        )

        this.log.jinfo({
            event : "CalculatedSpread",
            params: {
                ammPair,
                spread: spread.toFixed(),
                amount: amount.toFixed(),
            },
        })

        const maxHoldingBaseAsset = PerpService.fromWei((await amm.getMaxHoldingBaseAsset()).d) // aka personal cap
        let maxHoldingBaseAssetLeft = maxHoldingBaseAsset.sub(position.size.abs())
        maxHoldingBaseAssetLeft = maxHoldingBaseAssetLeft.lt(0) ? Big(0) : maxHoldingBaseAssetLeft // this might happen if somehow we reduce the personal cap
        this.log.jinfo({
            event: "PerpFiPersonalCap",
            params: {
                ammPair,
                maxHoldingBaseAssetLeft: +maxHoldingBaseAssetLeft,
                maxHoldingBaseAsset: +maxHoldingBaseAsset,
                currentPositionSize: +position.size,
            },
        })
        // Open positions if needed
        if (spread.lt(ammConfig.PERPFI_LONG_ENTRY_TRIGGER)) {
            const regAmount = this.calculateRegulatedPositionNotional(
                ammConfig,
                ammPrice,
                maxHoldingBaseAsset,
                quoteBalance,
                amount,
                position,
                Side.BUY,
            )
            const ftxPositionSizeAbs = this.calculateFTXPositionSize(ammConfig, regAmount, ftxPrice)
            if (ftxPositionSizeAbs.eq(Big(0))) {
                return
            }

            const positionChangedLog = await this.openPerpFiPosition(
                amm,
                ammPair,
                regAmount,
                ammConfig.PERPFI_LEVERAGE,
                Side.BUY,
            )
            await this.openFTXPosition(
                ammConfig.FTX_MARKET_ID,
                positionChangedLog.exchangedPositionSize.abs(),
                Side.SELL,
            )
        } else if (spread.gt(ammConfig.PERPFI_SHORT_ENTRY_TRIGGER)) {
            const regAmount = this.calculateRegulatedPositionNotional(
                ammConfig,
                ammPrice,
                maxHoldingBaseAsset,
                quoteBalance,
                amount,
                position,
                Side.SELL,
            )
            const ftxPositionSizeAbs = this.calculateFTXPositionSize(ammConfig, regAmount, ftxPrice)
            if (ftxPositionSizeAbs.eq(Big(0))) {
                return
            }

            const positionChangedLog = await this.openPerpFiPosition(
                amm,
                ammPair,
                regAmount,
                ammConfig.PERPFI_LEVERAGE,
                Side.SELL,
            )
            await this.openFTXPosition(
                ammConfig.FTX_MARKET_ID,
                positionChangedLog.exchangedPositionSize.abs(),
                Side.BUY,
            )
        } else {
            this.log.jinfo({
                event: "NotTriggered",
                params: {
                    spread,
                    ammConfig,
                },
            })
        }
    }

    getAmmPair(ammState: AmmProps): string {
        return `${ammState.baseAssetSymbol}-${ammState.quoteAssetSymbol}`
    }

    async fetchAmmPrice(amm: Amm): Promise<Big> {
        const ammState = await this.perpService.getAmmStates(amm.address)
        const ammPrice = ammState.quoteAssetReserve.div(ammState.baseAssetReserve)
        const ammPair = this.getAmmPair(ammState)
      
        this.log.jinfo({
            event: "PerpFiPrice",
            params: {
                ammPair: ammPair,
                price: ammPrice.toFixed(),
            },
        })
        return ammPrice
    }

    async fetchFtxPrice(ammConfig: AmmConfig): Promise<Big> {
        const ftxMarket = await this.ftxService.getMarket(ammConfig.FTX_MARKET_ID)
        const ftxPrice = ftxMarket.last!
        this.log.jinfo({
            event: "FtxPrice",
            params: {
                tokenPair: ammConfig.FTX_MARKET_ID,
                price: ftxPrice.toFixed(),
            },
        })
        return ftxPrice
    }

    calculateRegulatedPositionNotional(
        ammConfig: AmmConfig,
        ammPrice: Big,
        maxHoldingBaseAsset: Big,
        quoteBalance: Big,
        maxSlippageAmount: Big,
        position: Position,
        side: Side,
    ): Big {
        const personalCapInQuote = maxHoldingBaseAsset.mul(ammPrice).mul(1 - 5 / 100) // set a loose personal cap due to slippage
        const cap = min([ammConfig.ASSET_CAP, personalCapInQuote])
        let maxOpenNotional = Big(0)

        // Example
        // asset cap >> 1000
        // you have long position notional >> 900
        // you can short >> 1900 maximum
        if (position.size.gte(0) && side == Side.SELL) {
            maxOpenNotional = position.openNotional.add(cap)
        }

        // Example
        // asset cap >> 1000
        // you have short position notional >> 900
        // you can long >> 1900 maximum
        else if (position.size.lte(0) && side == Side.BUY) {
            maxOpenNotional = position.openNotional.add(cap)
        }

        // Example
        // asset cap >> 1000
        // you have long position notional >> 900
        // you can long >> 100 maximum
        else if (position.size.gte(0) && side == Side.BUY) {
            maxOpenNotional = cap.sub(position.openNotional)
            if (maxOpenNotional.lt(0)) {
                maxOpenNotional = Big(0)
            }
        }

        // Example
        // asset cap >> 1000
        // you have short position notional >> 900
        // you can short >> 100 maximum
        else if (position.size.lte(0) && side == Side.SELL) {
            maxOpenNotional = cap.sub(position.openNotional)
            if (maxOpenNotional.lt(0)) {
                maxOpenNotional = Big(0)
            }
        }

        let amount = maxSlippageAmount
        if (amount.gt(maxOpenNotional)) {
            amount = maxOpenNotional
            this.log.jinfo({
                event: "AmountPerpFiExceedCap",
                params: {
                    ammConfig,
                    side,
                    size: +position.size,
                    openNotional: +position.openNotional,
                    maxSlippageAmount: +maxSlippageAmount,
                    maxOpenNotional: +maxOpenNotional,
                    amount: +amount,
                    cap: +cap,
                    assetCap: +ammConfig.ASSET_CAP,
                    personalCapInQuote: +personalCapInQuote,
                },
            })
        }

        const feeSafetyMargin = ammConfig.ASSET_CAP.mul(this.perpfiFee).mul(3)
        if (amount.gt(quoteBalance.sub(feeSafetyMargin).mul(ammConfig.PERPFI_LEVERAGE))) {
            amount = quoteBalance.sub(feeSafetyMargin).mul(ammConfig.PERPFI_LEVERAGE)
        }

        if (amount.lt(ammConfig.PERPFI_MIN_TRADE_NOTIONAL)) {
            amount = Big(0)
            this.log.jinfo({
                event: "AmountNotReachPerpFiMinTradeNotional",
                params: {
                    ammConfig,
                    side,
                    size: +position.size,
                    openNotional: +position.openNotional,
                    maxSlippageAmount: +maxSlippageAmount,
                    maxOpenNotional: +maxOpenNotional,
                    feeSafetyMargin: +feeSafetyMargin,
                    amount: +amount,
                    cap: +cap,
                    assetCap: +ammConfig.ASSET_CAP,
                    personalCapInQuote: +personalCapInQuote,
                },
            })
        } else if (amount.eq(Big(0))) {
            this.log.jinfo({
                event: "AmountZero",
                params: {
                    ammConfig,
                    side,
                    size: +position.size,
                    openNotional: +position.openNotional,
                    maxSlippageAmount: +maxSlippageAmount,
                    maxOpenNotional: +maxOpenNotional,
                    feeSafetyMargin: +feeSafetyMargin,
                    amount: +amount,
                    cap: +cap,
                    assetCap: +ammConfig.ASSET_CAP,
                    personalCapInQuote: +personalCapInQuote,
                },
            })
        } else {
            this.log.jinfo({
                event: "AmountCalculated",
                params: {
                    ammConfig,
                    side,
                    size: +position.size,
                    openNotional: +position.openNotional,
                    maxSlippageAmount: +maxSlippageAmount,
                    maxOpenNotional: +maxOpenNotional,
                    feeSafetyMargin: +feeSafetyMargin,
                    amount: +amount,
                    cap: +cap,
                    assetCap: +ammConfig.ASSET_CAP,
                    personalCapInQuote: +personalCapInQuote,
                },
            })
        }
        return amount
    }

    calculateFTXPositionSize(ammConfig: AmmConfig, perpFiRegulatedPositionNotional: Big, ftxPrice: Big): Big {
        let ftxPositionSizeAbs = perpFiRegulatedPositionNotional.div(ftxPrice).abs().round(3) // round to FTX decimals
        if (ftxPositionSizeAbs.lt(ammConfig.FTX_MIN_TRADE_SIZE)) {
            ftxPositionSizeAbs = Big(0)
            this.log.jinfo({
                event: "PositionSizeNotReachFTXMinTradeSize",
                params: {
                    ammConfig,
                    ftxPositionSizeAbs: +ftxPositionSizeAbs,
                },
            })
        }
        return ftxPositionSizeAbs
    }

    static calcQuoteAssetNeeded(baseAssetReserve: Big, quoteAssetReserve: Big, price: Big): Big {
        // quoteAssetNeeded = sqrt(quoteAssetReserve * baseAssetReserve * price) - quoteAssetReserve
        const ammPrice = quoteAssetReserve.div(baseAssetReserve)
        if (ammPrice.eq(price)) return Big(0)
        return quoteAssetReserve.mul(baseAssetReserve).mul(price).sqrt().minus(quoteAssetReserve)
    }

    static calcMaxSlippageAmount(ammPrice: Big, maxSlippage: Big, baseAssetReserve: Big, quoteAssetReserve: Big): Big {
        const targetAmountSq = ammPrice.mul(new Big(1).add(maxSlippage)).mul(baseAssetReserve).mul(quoteAssetReserve)
        return targetAmountSq.sqrt().sub(quoteAssetReserve)
    }

    private async openPerpFiPosition(
        amm: Amm,
        ammPair: string,
        quoteAssetAmount: Big,
        leverage: Big,
        side: Side,
    ): Promise<PositionChangedLog> {
        const amount = quoteAssetAmount.div(leverage)
        const gasPrice = await this.ethService.getSafeGasPrice()

        const release = await this.nonceMutex.acquire()
        let tx
        try {
            tx = await this.perpService.openPosition(this.arbitrageur, amm.address, side, amount, leverage, Big(0), {
                nonce: this.nextNonce,
                gasPrice,
            })
            this.nextNonce++
        } finally {
            release()
        }

        this.log.jinfo({
            event: "OpenPerpFiPosition",
            params: {
                amm: amm.address,
                ammPair,
                side,
                quoteAssetAmount: +quoteAssetAmount,
                leverage: leverage.toFixed(),
                txHash: tx.hash,
                gasPrice: tx.gasPrice ? tx.gasPrice.toString() : null,
                nonce: tx.nonce,
            },
        })

        // I'm not sure why that field `events` isn't defined in    , but it does exist in the JS object.
        // The field might be removed in the future?
        // TODO: find another way to get `exchangedPositionSize`
        const txReceipt = (await tx.wait()) as any
        const event = txReceipt.events.filter((event: any) => event.event === "PositionChanged")[0]
        const positionChangedLog = this.perpService.toPositionChangedLog(event.args)
        return positionChangedLog
    }

    private async openFTXPosition(marketId: string, positionSizeAbs: Big, side: Side): Promise<void> {
        const payload: PlaceOrderPayload = {
            market: marketId,
            side: side === Side.BUY ? "buy" : "sell",
            price: null,
            size: parseFloat(positionSizeAbs.toFixed(3)), // rounding to FTX contract decimals
            type: "market",
        }

        this.log.jinfo({
            event: "OpenFTXPosition",
            params: payload,
        })
        await this.ftxService.placeOrder(this.ftxClient, payload)
    }

    async calculateTotalValue(amms: Amm[]): Promise<void> {
        let totalPositionValue = Big(0)
        for (let amm of amms) {
            const [position, unrealizedPnl] = await Promise.all([
                this.perpService.getPersonalPositionWithFundingPayment(amm.address, this.arbitrageur.address),
                this.perpService.getUnrealizedPnl(amm.address, this.arbitrageur.address, PnlCalcOption.SPOT_PRICE),
            ])
            totalPositionValue = totalPositionValue.add(position.margin).add(unrealizedPnl)
        }
        this.log.jinfo({
            event: "TotalAccountValue",
            params: {
                totalValue: +this.perpfiBalance.add(this.ftxAccountValue).add(totalPositionValue),
            },
        })
    }
}
