# Alpha Harness Accounts

A small local dashboard and CLI for running multiple Alpha Harness development profiles.
It is a sibling project; it does not modify the app's repository.

## Run the manager

Requires Python 3.11+, `uv`, and `npm`. By default, the manager expects this layout:

```text
Liz_harness/
  alpha-harness/
  alpha-harness-manager/
```

To start the dashboard, double-click `manager.py` or enter
```
python3 manager.py dashboard
```
If your Linux file manager asks, choose to run the executable file. Alternatively, use
`python3 manager.py` from this directory.

If Alpha Harness is elsewhere, set `ALPHA_HARNESS_REPO` to its repository directory.

From this directory, start or register an account:

```bash
python3 manager.py add john
```

The manager starts that account's backend and frontend in the background, then prints the
frontend URL. Open the dashboard at <http://127.0.0.1:5100> to see every account and its links,
start or stop profiles, or add another account from the page. The manager starts the dashboard
automatically when you use a CLI command.

```bash
python3 manager.py list
python3 manager.py start john
python3 manager.py stop john
python3 manager.py remove john
python3 manager.py dashboard
python3 manager.py shutdown
```

The power icon in the dashboard header stops every running profile without closing the dashboard.
The adjacent close icon returns to a standby page; **Start manager** restores the dashboard, and
running profiles stay available at their existing URLs. The local manager service remains ready in
the background to support that restart. To actually stop the manager service, run
`python3 manager.py shutdown`; this also leaves running profiles available. Individual profile cards
use start/stop and remove icons.

Account names are case-insensitive and are stored lowercase. Use letters, numbers, and internal
hyphens, up to 32 characters. `john` and `anne` each have their own database, catalog, encryption
key, backend process, and frontend process. The manager stores its account registry in
`~/.alpha-harness-manager/accounts.sqlite3` and process logs in `~/.alpha-harness-manager/logs/`.
If a process exits unexpectedly, the dashboard marks the profile as an error and points to those logs.

The dashboard binds only to `127.0.0.1`. Each app's frontend URL is shown and can be copied from its
row. A built-in `default` profile, displayed as **WorldQuant Brain**, is pinned at the top and can
be started or stopped, but not removed. It runs `uv run alpha-harness` in `backend/` and
`npm run dev` in `frontend/`; other profiles use the manager's explicit Uvicorn and Vite host options.
Each profile also displays its most recent start date and time in your local timezone.

## Port assignment

Numbered profiles such as `acc2` keep the app's existing convention. Other names receive a stable
backend and frontend port pair derived from the name. If a port is busy on a profile's first start,
the manager selects and saves another available pair. That profile keeps the same URL afterward;
if its saved ports are later occupied, the manager reports the conflict instead of silently changing
the URL. Each process is stopped by the manager using its own process group.
# crazy
# crazy
# crazy
