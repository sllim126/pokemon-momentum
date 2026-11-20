# Our own product pictures

For products TCGplayer has no picture for (cases, multi-packs, Master Ball cards and the like).

- Name each file after the TCGplayer product ID: `images/products/<productId>.jpg`
  (`.jpeg`, `.png` and `.webp` work too). The ID is the number in the product's TCGplayer
  address, `tcgplayer.com/product/<productId>/...`.
- Keep them small: about 400 px wide is plenty (the pages show them at 200-400 px).
- A file here beats the stand-in in `scripts/dashboards/product_image_standins.csv`; you can
  delete that product's stand-in row once its own picture is in.
- The site picks them up at the next nightly update, or straight away (product lists only) with
  `docker-compose exec -T pokemon-momentum python scripts/utilities/apply_product_image_overrides.py`.

Script and rules: `scripts/utilities/apply_product_image_overrides.py`.
