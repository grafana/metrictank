# go-murmur

Go programming language implementation of MurmurHash2, based on the work by [Austin Appleby](https://code.google.com/p/smhasher/).

## Performance

Looks roughly something like this:

    BenchmarkMurmurHash2     50000000   31.6 ns/op
    BenchmarkMurmurHash2A    50000000   35.3 ns/op
    BenchmarkMurmurHash64A   100000000  26.7 ns/op
    BenchmarkHash32_Murmur2  10000000   197 ns/op
    BenchmarkHash32_FNV1     10000000   155 ns/op
    BenchmarkHash32_FNV1a    10000000   156 ns/op

## Caveats

Always test code to make sure that it works as intended. Please submit pull requests if you find anything amiss.

## License

[MIT](LICENSE)