# ATLA Binder Planner Stack

Static nginx container for the planner, published at `https://market.poke6s.com/atla/`
through the existing Caddy reverse proxy.

## Layout

- `public/`: everything the site serves (`index.html` is the planner, `cards/` holds optional scans, `robots.txt`)
- `nginx/`: server config and security headers baked into the image
- `Dockerfile`: copies `public/` and `nginx/` into `nginx:1.27-alpine`
- `docker-compose.yml`: builds the image and listens on host port `8090`

Only `public/` is served. Compose files, nginx config, and `.md` files are not reachable from the web.

## Deploy on the server (192.168.1.240)

1. Copy this folder to the server, for example:

   ```powershell
   scp -r "G:\ATLA Binder" user@192.168.1.240:/opt/atla-binder
   ```

2. Build and start it, either with compose on the server:

   ```bash
   cd /opt/atla-binder && docker compose up -d --build
   ```

   or in Portainer: Stacks > Add stack > Repository (if the folder is in a git repo) so Portainer can run the build.
   The Web editor option cannot build because it has no access to `public/`.

3. Check it on the LAN: `http://192.168.1.240:8090/`

## Caddy

Add this inside the existing `market.poke6s.com { ... }` site block, then reload Caddy:

```caddy
redir /atla /atla/ 308
handle_path /atla/* {
	encode zstd gzip
	reverse_proxy 192.168.1.240:8090
}
```

`handle_path` strips `/atla` before proxying, and the planner uses relative paths, so no app changes are needed.
If Caddy runs in Docker on the same network as this container, `atla-binder-planner:80` can replace the IP and port,
and the `ports:` mapping can be removed.

Do not forward port 8090 on the router; only Caddy (80/443) should be exposed.

## Updating

Edit files on the PC, copy them to the server again, and rerun `docker compose up -d --build`.
Card scans go in `public/cards/<set>/` (see `public/cards/README.md`) and need the same rebuild.
The image build generates `cards/image-manifest.js` automatically, so browsers request only
scans that exist instead of probing several missing file extensions.

## Notes

- The site sends `noindex` headers and a `Disallow: /` robots.txt, so it stays unlisted.
- Collection tracking lives in each visitor's browser (`localStorage`). Every device and visitor has its own
  separate progress, and nobody can see or change yours. Use Export and Import to back up progress or move it.
- Scryfall prices are cached for 24 hours. Pagination and the two set downloads run sequentially with a delay,
  keeping refreshes below Scryfall's request-rate guidance.
