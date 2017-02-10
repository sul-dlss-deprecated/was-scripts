#!/bin/bash
container="$1"
prefix="$2"

echo "$grpre"

swift -A <url> -U <user:credentials> -K <pwd> --os-storage-url=<url>/$container list \
| grep "^$prefix/" \
| while read -r line; do swift -A <url> -U <user:credentials> -K <pwd> --os-storage-url=<url> download "$container" "$line"; done
