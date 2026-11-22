# tesseract.js, pinned copies for the admin's screenshot reader

Served to the Supplier Pricing page (`static/supplier-ocr.js`) because the admin's Content
Security Policy allows scripts from the admin only. Copied 2026-11-22 from the npm packages
(tarball SHA-1s checked against the registry):

| File | Package | From |
|---|---|---|
| `tesseract.min.js`, `worker.min.js` | tesseract.js 5.1.1 | `dist/` |
| `tesseract-core-simd-lstm.wasm.js` | tesseract.js-core 5.1.1 | package root (SIMD build; every current browser has SIMD) |
| `eng.traineddata.gz` | @tesseract.js-data/eng 1.0.0 | `4.0.0_best_int/` |

SHA-256:

```
a8e29918d098b2b06e1012bdaeffb4aec0445c5d5654709023e0bd1f442a80e8  tesseract.min.js
aca1229639fc9907d86f96e825955a2b7c5716d17f3bc3acd71f9c7ab66181fc  worker.min.js
ce20eda9533cbed1e6c2b4276fbae1e0adc61b6754b5513084be601787b457cf  tesseract-core-simd-lstm.wasm.js
45b4cb346724ac1774f1c36f42f182b887bcdb28ebe63e6fff90ac41f3fcff91  eng.traineddata.gz
```

`LICENSE` is tesseract.js's (Apache 2.0). To update, replace all four files from the same
versions of the three packages and update this table; the admin serves them with
`'wasm-unsafe-eval'` added to its policy (`OCR_CSP` in `admin/main.py`).
