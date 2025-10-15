name: Scrape MILA Publications

on:
  workflow_dispatch:
  schedule:
    - cron: "17 6 * * 1"   # chaque lundi 06:17 UTC

# IMPORTANT: autorise le push avec le GITHUB_TOKEN
permissions:
  contents: write

jobs:
  scrape:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          fetch-depth: 0           # nécessaire pour pousser
          persist-credentials: true

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Run scraper
        env:
          MILA_BASE_URL: "https://mila.quebec"
          MILA_PUBLICATION_PATHS: "/en/publications/,/fr/publications/"
          OUTPUT_DIR: "data"
          OUTPUT_XLSX: "data/mila_publications.xlsx"
        run: |
          mkdir -p data
          python scrape_mila_publications.py
          ls -lah data || true

      - name: Upload artifact (Excel)
        uses: actions/upload-artifact@v4
        with:
          name: mila_publications_xlsx
          path: data/mila_publications.xlsx
        # Ne pas échouer si le fichier est manquant (ex: zéro résultat)
        continue-on-error: true

      - name: Upload artifact (Ignored robots)
        uses: actions/upload-artifact@v4
        with:
          name: mila_ignored_by_robots
          path: data/ignored_by_robots.csv
        continue-on-error: true

      # Étape COMMIT: robuste et non bloquante
      - name: Commit result back to repo (safe)
        if: ${{ github.event_name != 'pull_request' }} # évite les PRs
        run: |
          set -e
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

          # Ajoute seulement les fichiers existants
          ADDED=0
          if [ -f "data/mila_publications.xlsx" ]; then
            git add data/mila_publications.xlsx
            ADDED=1
          fi
          if [ -f "data/ignored_by_robots.csv" ]; then
            git add data/ignored_by_robots.csv
            ADDED=1
          fi

          if [ "$ADDED" -eq 1 ]; then
            # Évite l'échec si rien n'a changé
            git commit -m "chore: update MILA publications + robots journal [skip ci]" || echo "No changes to commit"
            # Push protégé contre les erreurs (ex: protections de branche)
            git push || echo "Push skipped (branch protections or permissions)."
          else
            echo "No files to add; skipping commit/push."
          fi
