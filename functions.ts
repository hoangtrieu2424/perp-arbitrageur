import Big from "big.js"

export function requireThat(input: unknown, msg = "variable is false or undefined"): asserts input {
    if (!input) throw new Error(msg)
}

export function sleep(ms: number): Promise<unknown> {
    return new Promise(resolve => setTimeout(resolve, ms))
}

export function bn(n: number | string): Big {
    return new Big(n)
}

export function min(vs: Big[]): Big {
    return vs.reduce((prev, cur) => (cur.lt(prev) ? cur : prev))
}
