FROM nginx:1.27-alpine

COPY nginx/default.conf /etc/nginx/conf.d/default.conf
COPY nginx/security-headers.conf /etc/nginx/snippets/security-headers.conf
COPY public/ /usr/share/nginx/html/

# Tell the browser exactly which optional local scans exist. This avoids probing
# four possible extensions (and receiving four 404s) before using Scrydex.
RUN set -o pipefail; \
    find /usr/share/nginx/html/cards -type f \
      \( -iname '*.jpg' -o -iname '*.jpeg' -o -iname '*.png' -o -iname '*.webp' \) \
      -print | sed 's#^/usr/share/nginx/html/##' | sort | \
    awk 'BEGIN { printf "window.ATLA_LOCAL_IMAGES = new Set([" } \
         { gsub(/\\/, "\\\\"); gsub(/\"/, "\\\""); printf "%s\"cards/%s\"", separator, $0; separator="," } \
         END { print "]);" }' \
      > /usr/share/nginx/html/cards/image-manifest.js
