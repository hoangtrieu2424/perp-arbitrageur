import { BigNumber } from "@ethersproject/bignumber"
import { Block, WebSocketProvider } from "@ethersproject/providers"
import { parseUnits } from "@ethersproject/units"
import Big from "big.js"
import { Wallet, ethers } from "ethers"
import { Service } from "typedi"

import { Log } from "./Log"
import { ServerProfile } from "./ServerProfile"
import { sleep } from "./util"

@Service()
export class EthService {
    provider!: WebSocketProvider
    static readonly log = Log.getLogger(EthService.name)

    constructor(readonly serverProfile: ServerProfile) {
        this.provider = this.initProvider()
    }

    initProvider(): WebSocketProvider {
        const provider = new WebSocketProvider(this.serverProfile.web3Endpoint)
        provider._websocket.on("close", async (code: any) => {
            await EthService.log.warn(
                JSON.stringify({
                    event: "ReconnectWebSocket",
                    params: { code },
                }),
            )
            provider._websocket.terminate()
            await sleep(3000) // wait before reconnect
            this.provider = this.initProvider() // reconnect and replace the original provider
        })
        return provider
    }

    privateKeyToWallet(privateKey: string): Wallet {
        return new ethers.Wallet(privateKey, this.provider)
    }

    createContract<T>(address: string, abi: ethers.ContractInterface, signer?: ethers.Signer): T {
        return new ethers.Contract(address, abi, signer ? signer : this.provider) as unknown as T
    }

    async getBlock(blockNumber: number): Promise<Block> {
        return await this.provider.getBlock(blockNumber)
    }

    async getSafeGasPrice(): Promise<BigNumber> {
        for (let i = 0; i < 3; i++) {
            const gasPrice = Big((await this.provider.getGasPrice()).toString())
            if (gasPrice.gt(Big(0))) {
                return parseUnits(
                    gasPrice
                        .mul(1.2) // add 20% markup so the tx is more likely to pass
                        .toFixed(0),
                    0,
                )
            }
        }
        throw new Error("GasPrice is 0")
    }

    async getBalance(addr: string): Promise<Big> {
        const balance = await this.provider.getBalance(addr)
        return new Big(ethers.utils.formatEther(balance))
    }
}
