---
name: pagination
description: Use when writing React code in an app that renders a long or growing list - infinite scroll, a "load more" button, a searchable paged table, or any UI that accumulates results page by page. Covers the useIteratorPixel hook (paged pixel queries with a total count) and the useIteratorApi hook (paged REST or promise-based fetching with short-page detection) from @semoss/sdk/react, plus debouncing the search inputs that drive them. Do not use for a one-shot pixel fetch (usePixel, see app-bootstrap) or for choosing what pixel to run (see the database, room, and model skills).
---

# Paginated lists and infinite scroll

`@semoss/sdk/react` ships two accumulator hooks. Both keep every loaded page in memory, expose `next()` to fetch the following page, and `reset()` to start over. Pick by data source:

| Hook | Source | Knows the end because |
| --- | --- | --- |
| `useIteratorPixel` | a pixel that reports a **total count** | `data.length < totalCount` |
| `useIteratorApi` | any `(limit, offset) => Promise<T[]>` | a page came back **shorter than `limit`** |

If your pixel does not return a total count, do not force `useIteratorPixel` — wrap the `runPixel` call in a `fetchPage` function and use `useIteratorApi`'s short-page detection instead.

## useIteratorPixel — paged pixel with a total count

```tsx
import { useIteratorPixel } from "@semoss/sdk/react";

const { data, totalCount, isLoading, isError, error, hasMore, next, reset } =
  useIteratorPixel<RoomsResponse, Room>(
    (limit, offset) =>
      `GetWorkspaceRooms(workspaceId=["${id}"], limit=[${limit}], offset=[${offset}]);`,
    (response) => response.total_count, // where the total lives
    (response) => response.rooms,       // where the page's rows live
    { limit: 25 },
    [id],                               // reset + refetch when these change
  );
```

- The query builder receives `(limit, offset)` — always interpolate both, or every "page" returns the same rows and the list duplicates.
- The pixel must be a **single statement** (it runs through `usePixel`, which does not split on `;`).
- The last positional argument is a dependency list: when any value changes the hook resets to offset 0 and clears accumulated data. Put the search term, filters, and parent ids there.
- Options: `limit` (page size), `insightId` (defaults to the app's shared insight from context), `onSuccess(data, isLoadingMore)`, `onError(error)`.

## useIteratorApi — paged REST / promise fetching

```tsx
import { useIteratorApi } from "@semoss/sdk/react";
import { getProjectUsers } from "@semoss/sdk";

const { data, isLoading, hasMore, next, reset, update } = useIteratorApi<User>(
  async (limit, offset) => {
    const { members } = await getProjectUsers(projectId, false, search, undefined, limit, offset);
    return members;
  },
  { limit: 25, enabled: dialogOpen },
  [projectId, search],
);
```

- Paging stops when a page returns fewer than `limit` rows — no total count needed.
- `enabled: false` suspends fetching entirely (a dropdown that has not opened yet). Flipping it to `true` loads page 0. Do **not** toggle `enabled` to force a refetch — put the driving state in the deps array instead.
- `update(prev => next)` patches loaded rows in place with no network call and no scroll jump — use it after a confirmed mutation (e.g. remove one row after a delete) instead of `reset()`.
- `reset()` refetches from offset 0 but deliberately leaves the stale rows visible until the new page 0 arrives, so a search-term change does not flash the list to empty.

## Hard rules for both hooks

- **`limit` must be constant for the hook's lifetime.** Changing it at runtime re-fetches at the current offset without resetting and corrupts the accumulated pages. If page size is user-configurable, remount the component (`key={pageSize}`).
- **Changing deps resets; calling `next()` appends.** Never call `next()` from an effect that also watches the deps — let the scroll/button drive it. `next()` already no-ops while a page is loading or when the end is reached.
- Render the sentinel from state, e.g. show the "load more" trigger only when `hasMore && !isLoading`.

## Wiring an infinite scroll sentinel

```tsx
const sentinelRef = useRef<HTMLDivElement>(null);

useEffect(() => {
  const el = sentinelRef.current;
  if (!el) return;
  const observer = new IntersectionObserver(
    (entries) => entries[0].isIntersecting && next(),
    { rootMargin: "200px" },
  );
  observer.observe(el);
  return () => observer.disconnect();
}, [next]);

return (
  <div>
    {data.map((row) => <Row key={row.id} {...row} />)}
    {hasMore && <div ref={sentinelRef}>{isLoading ? "Loading..." : ""}</div>}
  </div>
);
```

`next` from both hooks is a stable callback, so it is safe in the effect deps.

## Debounced search driving the deps

A raw input value in the deps array refetches on every keystroke. Debounce the value, and pass the debounced value to both the fetcher and the deps:

```tsx
const [search, setSearch] = useState("");
const [debouncedSearch, setDebouncedSearch] = useState("");

useEffect(() => {
  const t = setTimeout(() => setDebouncedSearch(search), 300);
  return () => clearTimeout(t);
}, [search]);

const list = useIteratorApi(
  async (limit, offset) => fetchUsers(debouncedSearch, limit, offset),
  { limit: 25 },
  [debouncedSearch],
);
```

`@semoss/sdk/react` also exports `useDebouncedValue` and `useDebouncedCallback`, but both are marked deprecated (in favor of `@semoss/ui/next`, which plain apps may not have) — the inline effect above has no dependency and is the safe default.
