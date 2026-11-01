// The service worker: the page loads with no network. It keeps every file of its build, and serves the page for any
// path the server does not answer. The API and the socket always go to the network.

interface Event {
  waitUntil(work: Promise<unknown>): void
}
interface Fetch extends Event {
  request: Request
  respondWith(answer: Promise<Response>): void
}
interface Worker {
  addEventListener(type: 'install' | 'activate', listener: (event: Event) => void): void
  addEventListener(type: 'fetch', listener: (event: Fetch) => void): void
  skipWaiting(): Promise<void>
  clients: { claim(): Promise<void> }
  registration: { scope: string }
}

const worker = self as unknown as Worker
// The build writes these: every file of the build, and a name that changes with them.
const FILES = '__FILES__' as unknown as string[]
const CACHE = 'mdpad-__VERSION__'
const PAGE = new URL('index.html', worker.registration.scope).href

worker.addEventListener('install', (event) => {
  // The page itself too: the build's list has its scripts and styles, and the page is no output of the bundle.
  const urls = [PAGE, ...FILES.map((file) => new URL(file, worker.registration.scope).href)]
  event.waitUntil(
    caches
      .open(CACHE)
      .then((cache) => cache.addAll(urls))
      .then(() => worker.skipWaiting()),
  )
})

worker.addEventListener('activate', (event) => {
  const old = caches.keys().then((names) => Promise.all(names.filter((n) => n.startsWith('mdpad-') && n !== CACHE).map((n) => caches.delete(n))))
  event.waitUntil(old.then(() => worker.clients.claim()))
})

worker.addEventListener('fetch', (event) => {
  const { request } = event
  const url = new URL(request.url)
  if (request.method !== 'GET' || url.origin !== location.origin || url.pathname.startsWith('/api/')) return
  if (request.mode === 'navigate') {
    // The network first, so a new build shows at once; the kept page when the server cannot be reached.
    event.respondWith(fetch(request).catch(() => caches.match(PAGE).then((page) => page ?? Response.error())))
    return
  }
  event.respondWith(caches.match(request).then((kept) => kept ?? fetch(request)))
})
