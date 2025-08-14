name: edinet-tdnet cloud run

on:
  workflow_dispatch:
    inputs:
      start:        { description: '開始日 YYYY-MM-DD', required: true }
      end:          { description: '終了日 YYYY-MM-DD', required: true }
      codes:        { description: '会社名/4桁コード(カンマ区切り) 例: 7203,ソニー', required: false, default: '' }
      filetypes:    { description: 'EDINETファイル種別 csv,pdf,xbrl', required: false, default: 'pdf' }
      include_yuho: { description: '有報 yes/no', required: false, default: 'yes' }
      include_q:    { description: '四半期 yes/no', required: false, default: 'yes' }
      tdnet:        { description: 'TDnet(短信) yes/no', required: false, default: 'yes' }

permissions:
  contents: read

concurrency:
  group: edinet-tdnet
  cancel-in-progress: true

jobs:
  run:
    if: github.event_name == 'workflow_dispatch' || github.event_name == 'schedule'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      # --- 診断：この日のEDINET/TDnet件数を表示 ---
      - name: Probe counts (EDINET/TDnet)
        shell: bash
        env:
          EDINET_API_KEY: ${{ secrets.EDINET_API_KEY }}
        run: |
          set -e
          S="${{ github.event.inputs.start }}"
          echo "Start=$S"
          echo "---- EDINET (first day only) ----"
          curl -sG \
            --data-urlencode "date=$S" \
            --data-urlencode "type=2" \
            --data-urlencode "Subscription-Key=$EDINET_API_KEY" \
            "https://api.edinet-fsa.go.jp/api/v2/documents.json" \
          | python - <<'PY'
import sys, json
j = json.load(sys.stdin)
res = j.get("results", [])
print("results_count:", len(res))
if res:
    r = res[0]
    print("sample:", {k: r.get(k) for k in ("docID","secCode","filerName","formCode","docDescription")})
PY
          echo "---- TDNET (first day only) ----"
          curl -s "https://webapi.yanoshin.jp/webapi/tdnet/list/$(echo $S | tr -d -).atom" | grep -c '決算短信' || true

      - name: Run downloader
        env:
          EDINET_API_KEY: ${{ secrets.EDINET_API_KEY }}
          DEFAULT_CODES:  ${{ secrets.DEFAULT_CODES }}
        run: |
          START="${{ github.event.inputs.start }}"; END="${{ github.event.inputs.end }}"
          CODES="${{ github.event.inputs.codes }}"; FILES="${{ github.event.inputs.filetypes }}"
          YUHO="${{ github.event.inputs.include_yuho }}"; QTR="${{ github.event.inputs.include_q }}"; TD="${{ github.event.inputs.tdnet }}"
          if [ -z "$CODES" ]; then CODES="$DEFAULT_CODES"; fi
          python edinet_tdnet_downloader.py \
            --start "$START" --end "$END" \
            --edinet-filetypes "$FILES" \
            --include-yuho "$YUHO" --include-quarter "$QTR" \
            --tdnet "$TD" \
            --codes "$CODES"

      # --- 保険：0件でもArtifactsを出す ---
      - name: Ensure downloads folder exists
        run: |
          mkdir -p downloads
          if [ ! -e downloads/README.txt ]; then
            echo "No files retrieved in this run. Check dates/codes or API key." > downloads/README.txt
          fi

      - name: Upload artifacts
        uses: actions/upload-artifact@v4
        with:
          name: outputs-${{ github.run_id }}
          path: downloads
          if-no-files-found: warn
          retention-days: 90
