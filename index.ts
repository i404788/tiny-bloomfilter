import { XXH64 } from "xxh3-ts";
import { fastpopcnt } from "bigint-popcnt";

const toBufferLE = (num: bigint, width: number) => {
    const hex = num.toString(16);
    const buffer =
        Buffer.from(hex.padStart(width * 2, '0').slice(0, width * 2), 'hex');
    buffer.reverse();
    return buffer;
}

const toBigIntLE = (buf: Buffer) => {
    const reversed = Buffer.from(buf);
    reversed.reverse();
    const hex = reversed.toString('hex');
    if (hex.length === 0) {
      return BigInt(0);
    }
    return BigInt(`0x${hex}`);
}

const mask64 = ((1n << 64n)-1n)
function Rotl64(a: bigint, n: bigint) {
    return (a << n) | (a >> (64n - n))
}

export enum FilterComparison {
    Incompatible = -0xff,
    None = -1,
    Equal = 0,
    Larger = 1,
    Smaller = 2,
    // The (estimated) amount of elements are the same, but some of the elements are different
    Inequal = 3
}

export default class BloomFilter {
    bits: number;
    k: number;
    filter: bigint = 0n;
    popcnt: (v: bigint) => bigint

    // k is number of hashes per item
    constructor(bits: number, k: number) {
        this.bits = bits
        this.k = k;
        this.popcnt = fastpopcnt(BigInt(bits))
    }

    public add(v: string | Buffer) {
        this.filter |= BloomFilter.itemHash(v, this.bits, this.k)
    }

    public test(v: string | Buffer) {
        let l = BloomFilter.itemHash(v, this.bits, this.k)
        return (this.filter & l) === l
    }

    public compare(bloom: BloomFilter): FilterComparison {
        if (bloom.k !== this.k || bloom.bits !== this.bits) return FilterComparison.Incompatible;
        if (this.filter === bloom.filter) return FilterComparison.Equal
        const lsize = this.size()
        const rsize = bloom.size()
        if (lsize > rsize) return FilterComparison.Larger
        else if (lsize < rsize) return FilterComparison.Smaller
        else return FilterComparison.Inequal

    }

    public union(bloom: BloomFilter) {
        if (bloom.k !== this.k || bloom.bits !== this.bits) throw new Error('Cannot join bloomfilters, parameters incompatible.')
        this.filter |= bloom.filter
    }

    // Estimated cardinality.
    public size() {
        return - (this.bits / this.k) * Math.log(1 - (Number(this.popcnt(this.filter)) / this.bits));
    }

    public static fromBuffer(buf: Buffer): BloomFilter {
        const k = toBigIntLE(buf.slice(0, 2))
        const ofilter = toBigIntLE(buf.slice(2))
        const filter = new BloomFilter((buf.byteLength-2) * 8, Number(k))
        filter.filter = ofilter
        return filter
    }

    public toBuffer() {
        return Buffer.concat([toBufferLE(BigInt(this.k), 2), toBufferLE(this.filter, Math.ceil(this.bits/8))])
    }

    /**
     * Automatically gets the optimal bloomfilter for some collection
     * 
     * NOT recommended for adding more items afterwards
     * @param collection Set of ids to filter
     */
    public static fromCollection(collection: (string | Buffer)[]): BloomFilter {
        let {bits, k} = BloomFilter.OptimalParameters(collection.length)

        // To power of 2
        bits = 1 << Math.ceil(Math.log2(bits)) 
        // console.log(bits, k, collection.length)

        let bfilter = new BloomFilter(Math.round(bits), k)
        collection.map(x => bfilter.add(x))

        return bfilter
    }

    public static itemHash(v: string | Buffer, bits: number, k: number): bigint {
        let o = 0n
        let m = BigInt(bits)
        let buf = typeof v === 'string' ? Buffer.from(v) : v
        let a = XXH64(buf, 0n)
        if (a < 0n) a *= -1n
        a ^= 0x6740bca37be0516dn

        let delta = Rotl64(a, 17n) | 1n
        let _k = BigInt(k)
        for (let i = 0n; i < _k; ++i) {
            delta += i;
            let bit = a % m;
            o |= 1n << bit;
            a = (a + delta) & mask64
        }
        // console.log(o.toString(2))
        return o
    }

    public static ExpectedFalsePositives(bits: number, n: number) {
        return 0.61285 ** (bits / n)
    }

    public static OptimalK(bits: number, n: number) {
        return (bits / n) * Math.log(2)
    }

    public static OptimalParameters(n: number, fp = 0.001){
        // https://github.com/ArashPartow/bloom/blob/master/bloom_filter.hpp
        let min_m = Number.MAX_VALUE;
        let min_k = 0.0
        let k = 1.0

        while (k < 1000.0)
        {
           const numerator   = (- k * n);
           const denominator = Math.log(1.0 - Math.pow(fp, 1.0 / k));
  
           const curr_m = numerator / denominator;
  
           if (curr_m < min_m)
           {
              min_m = curr_m;
              min_k = k;
           }
  
           k += 1.0;
        }

        return {
            k: min_k < 1 ? 1 : min_k,
            bits: min_m < 8 ? 8 : min_m,
        }
    }
}
