name: MAUDE Monitor

on:
  schedule:
    - cron: '0 12 * * *'
  workflow_dispatch:

permissions:
  contents: write
  pages: write

env:
  FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true

jobs:
  monitor:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Create data directories
        run: mkdir -p data docs

      - name: Run MAUDE Monitor
        env:
          MAUDE_EMAIL_TO: ${{ secrets.MAUDE_EMAIL_TO }}
          MAUDE_EMAIL_FROM: ${{ secrets.MAUDE_EMAIL_FROM }}
          MAUDE_SMTP_PASSWORD: ${{ secrets.MAUDE_SMTP_PASSWORD }}
        run: python maude_monitor.py --html

      - name: Commit updated data
        run: |
          git config user.name "MAUDE Bot"
          git config user.email "maude-bot@users.noreply.github.com"
          git add data/ docs/
          git diff --cached --quiet || git commit -m "Update MAUDE data $(date +%Y-%m-%d)"
          git push
