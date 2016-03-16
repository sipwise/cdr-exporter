#!/bin/sh
error() { echo "$@"; exit 1; }
test -n "$1" || error "Usage: $0 <cdr-file>"
test -f "$1" || error "File '$1' not found"
BASECDR="$(basename "$1")"
TMPFILE="$(mktemp -t "cdr-md5.${BASECDR}.XXXXXXXXXX")"
MD5="$(tail -1 "$1")  ${TMPFILE}"
sed '$d' < "$1" > "${TMPFILE}"
echo "$MD5" | md5sum -c -
rm -f "${TMPFILE}"
