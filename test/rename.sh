#!/usr/bin/env bash

. ./_init.sh

mkdir "$mnt/test"
touch "$mnt/test/a"
mv "$mnt/test/a" "$mnt/test/b"
sync "$mnt/test/b"
ls -lah "$mnt/test/a" || echo -n
ls -lah "$mnt/test/b"
find "$mnt"
./target/debug/tool dump "$img"

python3 -c "import os; os.rename('$mnt/test/b', '$mnt/test/b')"

mkdir "$mnt/test/c"
python3 -c "import os; os.rename('$mnt/test', '$mnt/test/c')" && exit 1 || true

. ./test/_finish.sh
