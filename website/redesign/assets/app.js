// Vitals redesign — interactions
(function () {
  document.documentElement.classList.remove('no-js');

  // nav scroll state
  var nav = document.querySelector('.nav');
  function onScroll() {
    if (!nav) return;
    nav.classList.toggle('scrolled', window.scrollY > 12);
  }
  window.addEventListener('scroll', onScroll, { passive: true });
  onScroll();

  // mobile menu
  var burger = document.querySelector('.hamburger');
  var links = document.querySelector('.nav-links');
  if (burger && links) {
    burger.addEventListener('click', function () { links.classList.toggle('open'); });
  }

  // scroll reveals + counters
  var reveals = [].slice.call(document.querySelectorAll('[data-reveal]'));
  function runCounters(scope) {
    var nums = scope.querySelectorAll('.num[data-count]');
    [].forEach.call(nums, function (n) {
      if (n._done) return; n._done = true;
      var to = parseFloat(n.getAttribute('data-count'));
      var dec = parseInt(n.getAttribute('data-decimals') || '0', 10);
      var suffix = n.getAttribute('data-suffix') || '';
      var dur = 1400, start = performance.now();
      function fmt(v) { return dec > 0 ? v.toFixed(dec) : Math.round(v).toLocaleString('en-US'); }
      function tick(now) {
        var p = Math.min(1, (now - start) / dur);
        var e = 1 - Math.pow(1 - p, 3);
        n.textContent = fmt(to * e) + suffix;
        if (p < 1) requestAnimationFrame(tick);
      }
      requestAnimationFrame(tick);
    });
  }
  if ('IntersectionObserver' in window) {
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (en) {
        if (en.isIntersecting) {
          en.target.classList.add('in');
          runCounters(en.target);
          io.unobserve(en.target);
        }
      });
    }, { threshold: 0.15, rootMargin: '0px 0px -8% 0px' });
    reveals.forEach(function (el) { io.observe(el); });
    // safety: never leave content hidden if IO/transition stalls
    setTimeout(function () { reveals.forEach(function (el) { el.classList.add('in'); }); }, 2500);
  } else {
    reveals.forEach(function (el) { el.classList.add('in'); });
  }

  // TOC active state
  var tocLinks = [].slice.call(document.querySelectorAll('.toc-links a'));
  var headings = tocLinks.map(function (a) {
    var id = a.getAttribute('href').slice(1);
    return document.getElementById(id);
  }).filter(Boolean);
  if (headings.length && 'IntersectionObserver' in window) {
    var seen = {};
    var tio = new IntersectionObserver(function (entries) {
      entries.forEach(function (en) { seen[en.target.id] = en.isIntersecting ? en.intersectionRatio : 0; });
      var best = null, bestR = 0;
      headings.forEach(function (h) { if ((seen[h.id] || 0) > bestR) { bestR = seen[h.id]; best = h.id; } });
      if (best) {
        tocLinks.forEach(function (a) { a.classList.toggle('active', a.getAttribute('href') === '#' + best); });
      }
    }, { rootMargin: '-80px 0px -70% 0px', threshold: [0, .25, .5, 1] });
    headings.forEach(function (h) { tio.observe(h); });
  }
})();
