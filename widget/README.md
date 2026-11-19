# widget/

The DMS bar widget, later. It shows:

- the codec in use (`sbc`, and later `aac` if it works),
- the earbud battery,
- the number of incidents today.

## Data source

The widget must not scrape the human output of `btguard status`. Add
`btguard status --json` when the watcher lands, and read that. A state file is
also acceptable if the widget cannot start a process.

## Not written yet

DMS is Quickshell/QML. Find the existing widget API on this machine before you
write code. Do not guess the API.
