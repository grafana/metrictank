Configparser
============

This is a fork of https://github.com/alyu/configparser

The original libray focuses on parsing ini files into memory, and re-serializing to disk while preserving comments and whitespace.
To achieve that, the in-memory has some quirks which most people probably don't want.

This fork breaks the writing config files functionality in order to get a more sensible config reading experience.
That said, there are still plenty of quirks, making this only useful for a handful of carefully picked use cases.

**You should probably not use this library**

## Known quirks and problems (mainly inherited from upstream):

* Since values are unquoted strings, it is effectively impossibly to truly distinguish comments from values.
  We simply split keys from values at the first '=' and consider any "#" to mark a comment. Any other chars are allowed.
* parsed values preserve file comments (but there's now an api to strip them. see below)
* empty section names are legal.
* section markers like `[[[foo[][]` are legal, though hard to reason about. (this one results in a section named `foo[`)
* sections without any options are legal.
* Any characters are allowed in section names
* epmty lines result in options with empty names
* Lines with nothing but comments result in "options" with the whole line (including comment delimiter) as name.

Most of these issues can, and should, be worked around in the caller by doing strict validation checking, based on your use case.

## Main differences with upstream

* [parsed section names no longer include file comments](https://github.com/alyu/configparser/issues/11)
* global section is kept separate so you can distinguish it from a section named "global".
* stricter validation and parsing of section headers
* add method to retrieve values without comments (ValueOfWithoutComments() )
* add lots of unit tests (see `extra_test.go`)
* only "=" is allowed as key-value delimiter (not ":" because our values may contain it)
* only "#" is allowed to start comments (not ";" because our values may contain it)
