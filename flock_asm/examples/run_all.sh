#! /bin/bash

set -eo pipefail

cd "$(dirname "$0")"/..

cargo build --release
for file in examples/*.asm; do
  RUST_BACKRACE=1 cargo run --release -- $file
done
