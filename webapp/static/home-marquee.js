(function () {
    const prefersReducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (prefersReducedMotion) return;

    document.querySelectorAll("[data-marquee]").forEach((windowNode) => {
        const track = windowNode.querySelector(".marquee-track");
        if (!track || track.children.length < 2) return;

        track.innerHTML += track.innerHTML;
        let position = 0;
        let paused = false;

        windowNode.addEventListener("mouseenter", () => { paused = true; });
        windowNode.addEventListener("mouseleave", () => { paused = false; });

        function step() {
            if (!paused) {
                position += 0.35;
                if (position >= track.scrollWidth / 2) position = 0;
                track.style.transform = `translateX(-${position}px)`;
            }
            window.requestAnimationFrame(step);
        }

        window.requestAnimationFrame(step);
    });
}());
