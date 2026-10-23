"use strict";

// Positions are fractions of the crop's side, so resizing never changes the crop.
window.AvatarCropper = class AvatarCropper {
  constructor() { this.revision = 0; this.clear(); }
  get active() { return !!this.image; }
  get minZoom() { return Math.min(this.image.width, this.image.height) / Math.hypot(this.image.width, this.image.height); }

  async load(file) {
    const revision = ++this.revision;
    const url = URL.createObjectURL(file);
    try {
      const image = new Image();
      await new Promise((resolve, reject) => { image.onload = resolve; image.onerror = reject; image.src = url; });
      if (!image.naturalWidth || image.naturalWidth * image.naturalHeight > 16000000) throw new Error("invalid_avatar");
      if (revision !== this.revision) { URL.revokeObjectURL(url); return false; }
      if (this.url) URL.revokeObjectURL(this.url);
      this.url = url; this.image = image;
      this.zoom = 1; this.x = 0; this.y = 0;
      return true;
    } catch (error) { URL.revokeObjectURL(url); throw error; }
  }

  clear() {
    ++this.revision;
    if (this.url) URL.revokeObjectURL(this.url);
    this.url = ""; this.image = null; this.root = null;
    this.zoom = 1; this.x = 0; this.y = 0;
  }

  limit() {
    const scale = this.zoom / Math.min(this.image.width, this.image.height);
    const maxX = Math.max(0, (this.image.width * scale - 1) / 2);
    const maxY = Math.max(0, (this.image.height * scale - 1) / 2);
    this.x = Math.max(-maxX, Math.min(maxX, this.x));
    this.y = Math.max(-maxY, Math.min(maxY, this.y));
  }

  setZoom(value, anchor = { x: 0, y: 0 }) {
    const zoom = Math.max(this.minZoom, Math.min(4, value));
    const ratio = zoom / this.zoom;
    this.x = anchor.x - (anchor.x - this.x) * ratio;
    this.y = anchor.y - (anchor.y - this.y) * ratio;
    this.zoom = zoom; this.limit(); this.paint();
  }

  fit() { this.zoom = this.minZoom; this.x = 0; this.y = 0; this.paint(); }
  reset() { this.zoom = 1; this.x = 0; this.y = 0; this.paint(); }

  draw(canvas) {
    const size = canvas.width;
    const context = canvas.getContext("2d");
    const scale = size * this.zoom / Math.min(this.image.width, this.image.height);
    const width = this.image.width * scale, height = this.image.height * scale;
    context.clearRect(0, 0, size, size);
    context.drawImage(this.image, (size - width) / 2 + this.x * size, (size - height) / 2 + this.y * size, width, height);
  }

  paint() {
    if (!this.active || !this.root?.isConnected) return;
    this.draw(this.root.querySelector("canvas"));
    this.root.querySelector("input[type=range]").value = this.zoom;
    this.root.querySelector("output").textContent = `${Math.round(this.zoom * 100)}%`;
  }

  bind(root, disabled = false) {
    this.root = root;
    const stage = root.querySelector(".avatar-crop-stage");
    const slider = root.querySelector("input[type=range]");
    slider.min = this.minZoom;
    this.paint();
    if (disabled) return;
    const pointers = new Map();
    let start = null;
    const point = event => {
      const box = stage.getBoundingClientRect();
      return { x: (event.clientX - box.left) / box.width - 0.5, y: (event.clientY - box.top) / box.height - 0.5 };
    };
    const snapshot = () => { start = { points: [...pointers.values()], zoom: this.zoom, x: this.x, y: this.y }; };
    stage.addEventListener("pointerdown", event => {
      if (event.pointerType === "mouse" && event.button !== 0) return;
      stage.setPointerCapture(event.pointerId);
      pointers.set(event.pointerId, point(event)); snapshot();
      event.preventDefault();
    });
    stage.addEventListener("pointermove", event => {
      if (!pointers.has(event.pointerId)) return;
      pointers.set(event.pointerId, point(event));
      const points = [...pointers.values()];
      if (points.length >= 2 && start.points.length >= 2) {
        const distance = pair => Math.hypot(pair[0].x - pair[1].x, pair[0].y - pair[1].y);
        const mid = pair => ({ x: (pair[0].x + pair[1].x) / 2, y: (pair[0].y + pair[1].y) / 2 });
        const before = mid(start.points), after = mid(points);
        const zoom = Math.max(this.minZoom, Math.min(4, start.zoom * distance(points) / Math.max(0.001, distance(start.points))));
        this.x = after.x - (before.x - start.x) * zoom / start.zoom;
        this.y = after.y - (before.y - start.y) * zoom / start.zoom;
        this.zoom = zoom;
      } else {
        this.x = start.x + points[0].x - start.points[0].x;
        this.y = start.y + points[0].y - start.points[0].y;
      }
      this.limit(); this.paint();
    });
    const release = event => { pointers.delete(event.pointerId); snapshot(); };
    for (const name of ["pointerup", "pointercancel", "lostpointercapture"]) stage.addEventListener(name, release);
    stage.addEventListener("wheel", event => {
      event.preventDefault(); this.setZoom(this.zoom * Math.exp(-event.deltaY * 0.002), point(event));
    }, { passive: false });
    stage.addEventListener("keydown", event => {
      const moves = { ArrowLeft: [-0.03, 0], ArrowRight: [0.03, 0], ArrowUp: [0, -0.03], ArrowDown: [0, 0.03] };
      if (moves[event.key]) {
        event.preventDefault(); this.x += moves[event.key][0]; this.y += moves[event.key][1]; this.limit(); this.paint();
      } else if (["+", "=", "-"].includes(event.key)) {
        event.preventDefault(); this.setZoom(this.zoom * (event.key === "-" ? 0.9 : 1.1));
      }
    });
    slider.addEventListener("input", () => this.setZoom(Number(slider.value)));
  }

  async toFile() {
    const canvas = document.createElement("canvas");
    canvas.width = canvas.height = 768;
    this.draw(canvas);
    const blob = await new Promise(resolve => canvas.toBlob(resolve, "image/png"));
    if (!blob) throw new Error("invalid_avatar");
    return new File([blob], "avatar.png", { type: "image/png" });
  }
};
