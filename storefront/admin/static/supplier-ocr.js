// Supplier Pricing: read a screenshot's text into the price-list box, in the browser.
//
// The admin's only script. The page works without it (paste text, then "Read the text"); this
// just fills the text box from a picture, the way the old market page did. It uses tesseract.js
// 5.1.1 served from /static/vendor/tesseract/ (README there), because the admin's Content
// Security Policy allows scripts from the admin itself only; nothing is sent anywhere, the
// picture is read on this computer. Those files are served with 'wasm-unsafe-eval' added to the
// policy (admin/main.py, OCR_CSP), which the reader's WebAssembly needs.
(() => {
  const box = document.querySelector(".sp-ocr");
  const input = document.getElementById("sp-shot");
  const status = document.getElementById("sp-ocr-status");
  const text = document.getElementById("sp-text");
  if (!box || !input || !status || !text || !window.Tesseract || !window.WebAssembly) return;
  const base = "/static/vendor/tesseract/";
  box.hidden = false;

  let worker = null;   // created on first use (about 7 MB to load), then kept for more screenshots
  function getWorker() {
    if (!worker) {
      worker = window.Tesseract.createWorker("eng", 1, {
        workerPath: base + "worker.min.js",
        corePath: base + "tesseract-core-simd-lstm.wasm.js",
        langPath: base.slice(0, -1),
        workerBlobURL: false,   // a blob: worker would need the policy loosened further
        logger: (m) => {
          if (m.status === "recognizing text") status.textContent = `Reading… ${Math.round(m.progress * 100)}%`;
        },
      });
    }
    return worker;
  }

  input.addEventListener("change", async () => {
    const file = input.files && input.files[0];
    if (!file) return;
    input.disabled = true;
    status.textContent = "Loading the reader (a few seconds the first time)…";
    try {
      const reader = await getWorker();
      const { data } = await reader.recognize(file);
      const read = (data && data.text ? data.text : "").trim();
      if (!read) {
        status.textContent = `No text found in ${file.name}.`;
        return;
      }
      // Added to what's there, so several screenshots of one list can be read in turn.
      text.value = text.value.trim() ? `${text.value.trim()}\n${read}` : read;
      const source = document.querySelector('input[name="source"]');
      if (source && !source.value) source.value = file.name;
      status.textContent = `Read ${read.split("\n").length} lines from ${file.name}. Check them, then Read the text.`;
    } catch (err) {
      worker = null;
      status.textContent = `Couldn't read ${file.name}: ${err && err.message ? err.message : err}`;
    } finally {
      input.disabled = false;
      input.value = "";
    }
  });
})();
