#!/usr/bin/env python3
"""Turn the raw cdylib (`librust_db_driver.so`/`.dylib`/`.dll`) into a loadable
`.duckdb_extension` by appending DuckDB's metadata footer.

This is a minimal, self-contained implementation of the well-documented DuckDB
extension metadata format (a WebAssembly-style custom-section header, eight
32-byte fields, and a 256-byte zero signature for unsigned extensions). For the
official distribution build the project uses duckdb/extension-ci-tools; this
script just makes local end-to-end testing a one-liner.

Usage:
  python3 scripts/package_extension.py \
      --library target/release/librust_db_driver.so \
      --out /tmp/rust_db_driver.duckdb_extension \
      --platform linux_amd64 --capi-version v1.2.0 --extension-version v0.1.0
"""
import argparse
import shutil


def start_signature() -> bytes:
    # WebAssembly custom-section preamble so the same footer works for Wasm too.
    b = b"\x00"               # custom section id
    b += int(147).to_bytes(1, "big")  # section length (LEB128, low byte)
    b += int(4).to_bytes(1, "big")    # section length (high byte)
    b += int(16).to_bytes(1, "big")   # name length
    b += b"duckdb_signature"          # 16-byte section name
    b += int(128).to_bytes(1, "big")  # payload length 512 (LEB128 low)
    b += int(4).to_bytes(1, "big")    # payload length 512 (high)
    return b


def field(value: str) -> bytes:
    enc = value.encode("ascii")
    if len(enc) > 32:
        raise ValueError(f"metadata field too long (>32 bytes): {value!r}")
    return enc + b"\x00" * (32 - len(enc))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--library", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--platform", required=True, help="e.g. linux_amd64")
    p.add_argument("--capi-version", required=True, help="C API version, e.g. v1.2.0")
    p.add_argument("--extension-version", default="v0.0.0")
    p.add_argument("--abi-type", default="C_STRUCT")
    args = p.parse_args()

    tmp = args.out + ".tmp"
    shutil.copyfile(args.library, tmp)
    with open(tmp, "ab") as f:
        f.write(start_signature())
        f.write(field(""))                       # FIELD8 (unused)
        f.write(field(""))                       # FIELD7 (unused)
        f.write(field(""))                       # FIELD6 (unused)
        f.write(field(args.abi_type))            # FIELD5 abi_type
        f.write(field(args.extension_version))   # FIELD4 extension_version
        f.write(field(args.capi_version))        # FIELD3 duckdb / C API version
        f.write(field(args.platform))            # FIELD2 platform
        f.write(field("4"))                      # FIELD1 header signature marker
        f.write(b"\x00" * 256)                   # unsigned: empty signature
    shutil.move(tmp, args.out)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
