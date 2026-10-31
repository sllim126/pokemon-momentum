// Small progressive enhancements. Every page works without this file.
// - sort menu submits on change;
// - product photos: thumbnails swap the main photo, clicking the main photo opens a zoom dialog.
// The cart (Phase 2) will live here too, kept in localStorage.
"use strict";

document.querySelectorAll("select[data-autosubmit]").forEach((select) => {
  select.addEventListener("change", () => select.form.submit());
});

const zoom = document.getElementById("zoom");
const mainLink = document.querySelector(".main-img[data-zoom]");
if (zoom && mainLink && typeof zoom.showModal === "function") {
  const mainImg = mainLink.querySelector("img");
  const thumbs = [...document.querySelectorAll(".thumbs a[data-swap]")];
  if (thumbs.length) thumbs[0].setAttribute("aria-current", "true");
  thumbs.forEach((thumb) => {
    thumb.addEventListener("click", (event) => {
      event.preventDefault();
      mainLink.href = thumb.href;
      mainImg.src = thumb.href;
      mainImg.alt = thumb.querySelector("img").alt;
      thumbs.forEach((t) => t.removeAttribute("aria-current"));
      thumb.setAttribute("aria-current", "true");
    });
  });
  mainLink.addEventListener("click", (event) => {
    event.preventDefault();
    // The <img> is created on first open so the page has no empty image before then.
    const img = zoom.querySelector("img") || zoom.appendChild(document.createElement("img"));
    img.src = mainLink.href;
    img.alt = mainImg.alt;
    zoom.showModal();
  });
  // Clicking the dark area around the photo closes it, like most shops.
  zoom.addEventListener("click", (event) => { if (event.target === zoom) zoom.close(); });
}
