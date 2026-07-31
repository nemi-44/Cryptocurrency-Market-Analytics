"""Streamlit entrypoint.

Run with:
    streamlit run dashboard_app.py -- --local-json .runtime/latest.json
"""

from crypto_analytics.dashboard import main


if __name__ == "__main__":
    raise SystemExit(main())

