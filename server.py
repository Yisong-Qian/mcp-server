import json
import os
import asyncio
import httpx
import logging
import sys
from typing import Any
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv

# Configure logging to write to stderr
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("financial-datasets-mcp")

# Initialize FastMCP server
mcp = FastMCP("financial-datasets")

# Load API key at startup and log a safe status
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(env_path, override=True)
api_key_at_startup = os.environ.get("FINANCIAL_DATASETS_API_KEY", "NOT_FOUND")
if api_key_at_startup == "NOT_FOUND":
    logger.warning("API key not found at startup")
else:
    logger.info(
        "API key loaded at startup: length=%s preview=%s...%s",
        len(api_key_at_startup),
        api_key_at_startup[:4],
        api_key_at_startup[-4:],
    )

# Constants
FINANCIAL_DATASETS_API_BASE = "https://api.financialdatasets.ai"


# Helper function to make API requests
async def make_request_with_client(url: str, client: httpx.AsyncClient) -> dict[str, Any] | None:
    """Make a request to the Financial Datasets API with proper error handling."""
    headers = {}
    if api_key := os.environ.get("FINANCIAL_DATASETS_API_KEY"):
        headers["X-API-KEY"] = api_key

    try:
        response = await client.get(url, headers=headers, timeout=30.0)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"Error": str(e)}


async def make_request(url: str) -> dict[str, Any] | None:
    """Make a request to the Financial Datasets API with proper error handling."""
    async with httpx.AsyncClient() as client:
        return await make_request_with_client(url, client)


def normalize_tickers(tickers: list[str]) -> list[str]:
    """Normalize ticker input while preserving order."""
    clean_tickers = []
    seen = set()
    for ticker in tickers:
        clean_ticker = ticker.strip().upper()
        if clean_ticker and clean_ticker not in seen:
            clean_tickers.append(clean_ticker)
            seen.add(clean_ticker)
    return clean_tickers


def clamp(value: int, min_value: int, max_value: int) -> int:
    return min(max(value, min_value), max_value)


@mcp.tool()
async def get_income_statements(
    ticker: str,
    period: str = "annual",
    limit: int = 4,
) -> str:
    """Get income statements for a company.

    Args:
        ticker: Ticker symbol of the company (e.g. AAPL, GOOGL)
        period: Period of the income statement (e.g. annual, quarterly, ttm)
        limit: Number of income statements to return (default: 4)
    """
    # Fetch data from the API
    url = f"{FINANCIAL_DATASETS_API_BASE}/financials/income-statements/?ticker={ticker}&period={period}&limit={limit}"
    data = await make_request(url)

    # Check if data is found
    if not data:
        return "Unable to fetch income statements or no income statements found."

    # Extract the income statements
    income_statements = data.get("income_statements", [])

    # Check if income statements are found
    if not income_statements:
        return "Unable to fetch income statements or no income statements found."

    # Stringify the income statements
    return json.dumps(income_statements, indent=2)


@mcp.tool()
async def get_balance_sheets(
    ticker: str,
    period: str = "annual",
    limit: int = 4,
) -> str:
    """Get balance sheets for a company.

    Args:
        ticker: Ticker symbol of the company (e.g. AAPL, GOOGL)
        period: Period of the balance sheet (e.g. annual, quarterly, ttm)
        limit: Number of balance sheets to return (default: 4)
    """
    # Fetch data from the API
    url = f"{FINANCIAL_DATASETS_API_BASE}/financials/balance-sheets/?ticker={ticker}&period={period}&limit={limit}"
    data = await make_request(url)

    # Check if data is found
    if not data:
        return "Unable to fetch balance sheets or no balance sheets found."

    # Extract the balance sheets
    balance_sheets = data.get("balance_sheets", [])

    # Check if balance sheets are found
    if not balance_sheets:
        return "Unable to fetch balance sheets or no balance sheets found."

    # Stringify the balance sheets
    return json.dumps(balance_sheets, indent=2)


@mcp.tool()
async def get_cash_flow_statements(
    ticker: str,
    period: str = "annual",
    limit: int = 4,
) -> str:
    """Get cash flow statements for a company.

    Args:
        ticker: Ticker symbol of the company (e.g. AAPL, GOOGL)
        period: Period of the cash flow statement (e.g. annual, quarterly, ttm)
        limit: Number of cash flow statements to return (default: 4)
    """
    # Fetch data from the API
    url = f"{FINANCIAL_DATASETS_API_BASE}/financials/cash-flow-statements/?ticker={ticker}&period={period}&limit={limit}"
    data = await make_request(url)

    # Check if data is found
    if not data:
        return "Unable to fetch cash flow statements or no cash flow statements found."

    # Extract the cash flow statements
    cash_flow_statements = data.get("cash_flow_statements", [])

    # Check if cash flow statements are found
    if not cash_flow_statements:
        return "Unable to fetch cash flow statements or no cash flow statements found."

    # Stringify the cash flow statements
    return json.dumps(cash_flow_statements, indent=2)


@mcp.tool()
async def get_current_stock_price(ticker: str) -> str:
    """Get the current / latest price of a company.

    Args:
        ticker: Ticker symbol of the company (e.g. AAPL, GOOGL)
    """
    # Fetch data from the API
    url = f"{FINANCIAL_DATASETS_API_BASE}/prices/snapshot/?ticker={ticker}"
    data = await make_request(url)

    # Check if data is found
    if not data:
        return "Unable to fetch current price or no current price found."

    # Extract the current price
    snapshot = data.get("snapshot", {})

    # Check if current price is found
    if not snapshot:
        return "Unable to fetch current price or no current price found."

    # Stringify the current price
    return json.dumps(snapshot, indent=2)


@mcp.tool()
async def get_current_stock_prices(
    tickers: list[str],
    max_concurrency: int = 20,
) -> str:
    """Get the current / latest prices for multiple companies concurrently.

    Args:
        tickers: Ticker symbols of the companies (e.g. ["AAPL", "MSFT", "NVDA"])
        max_concurrency: Maximum concurrent API requests to run (default: 20)
    """
    clean_tickers = normalize_tickers(tickers)
    if not clean_tickers:
        return "Unable to fetch current prices: no valid tickers provided."

    concurrency = clamp(max_concurrency, 1, 50)
    semaphore = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient() as client:
        async def fetch_snapshot(ticker: str) -> dict[str, Any]:
            url = f"{FINANCIAL_DATASETS_API_BASE}/prices/snapshot/?ticker={ticker}"
            async with semaphore:
                data = await make_request_with_client(url, client)

            if not data:
                return {"ticker": ticker, "error": "Unable to fetch current price."}
            if error := data.get("Error"):
                return {"ticker": ticker, "error": error}

            snapshot = data.get("snapshot", {})
            if not snapshot:
                return {"ticker": ticker, "error": "No current price found."}

            return snapshot

        snapshots = await asyncio.gather(
            *(fetch_snapshot(ticker) for ticker in clean_tickers)
        )

    return json.dumps(snapshots, indent=2)


@mcp.tool()
async def get_stock_screen_data(
    tickers: list[str],
    period: str = "annual",
    financial_limit: int = 4,
    news_limit: int = 5,
    max_concurrency: int = 20,
) -> str:
    """Get screening data for multiple companies concurrently.

    Fetches current price, income statements, balance sheets, cash flow
    statements, and recent company news in one tool call.

    Args:
        tickers: Ticker symbols of the companies (e.g. ["AAPL", "MSFT", "NVDA"])
        period: Financial statement period (e.g. annual, quarterly, ttm)
        financial_limit: Number of financial statements per type to return (default: 4)
        news_limit: Number of news items per ticker to return (default: 5)
        max_concurrency: Maximum concurrent API requests to run (default: 20)
    """
    clean_tickers = normalize_tickers(tickers)
    if not clean_tickers:
        return "Unable to fetch stock screen data: no valid tickers provided."

    statement_limit = clamp(financial_limit, 1, 10)
    company_news_limit = clamp(news_limit, 0, 20)
    concurrency = clamp(max_concurrency, 1, 50)
    semaphore = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient() as client:
        async def fetch_json(url: str) -> dict[str, Any] | None:
            async with semaphore:
                return await make_request_with_client(url, client)

        async def fetch_section(
            ticker: str,
            label: str,
            url: str,
            response_key: str,
            empty_value: Any,
        ) -> tuple[str, Any, str | None]:
            data = await fetch_json(url)
            if not data:
                return label, empty_value, f"{label}: no data returned"
            if error := data.get("Error"):
                return label, empty_value, f"{label}: {error}"

            payload = data.get(response_key, empty_value)
            if label == "news" and isinstance(payload, list):
                payload = payload[:company_news_limit]
            if payload in ({}, []):
                return label, empty_value, f"{label}: no {response_key} found"

            return label, payload, None

        async def fetch_ticker_data(ticker: str) -> dict[str, Any]:
            endpoints = [
                (
                    "price",
                    f"{FINANCIAL_DATASETS_API_BASE}/prices/snapshot/?ticker={ticker}",
                    "snapshot",
                    None,
                ),
                (
                    "income_statements",
                    f"{FINANCIAL_DATASETS_API_BASE}/financials/income-statements/?ticker={ticker}&period={period}&limit={statement_limit}",
                    "income_statements",
                    [],
                ),
                (
                    "balance_sheets",
                    f"{FINANCIAL_DATASETS_API_BASE}/financials/balance-sheets/?ticker={ticker}&period={period}&limit={statement_limit}",
                    "balance_sheets",
                    [],
                ),
                (
                    "cash_flow_statements",
                    f"{FINANCIAL_DATASETS_API_BASE}/financials/cash-flow-statements/?ticker={ticker}&period={period}&limit={statement_limit}",
                    "cash_flow_statements",
                    [],
                ),
                (
                    "news",
                    f"{FINANCIAL_DATASETS_API_BASE}/news/?ticker={ticker}",
                    "news",
                    [],
                ),
            ]
            sections = await asyncio.gather(
                *(
                    fetch_section(ticker, label, url, response_key, empty_value)
                    for label, url, response_key, empty_value in endpoints
                )
            )

            result: dict[str, Any] = {"ticker": ticker}
            errors = []
            for label, payload, error in sections:
                result[label] = payload
                if error:
                    errors.append(error)
            if errors:
                result["errors"] = errors

            return result

        screen_data = await asyncio.gather(
            *(fetch_ticker_data(ticker) for ticker in clean_tickers)
        )

    return json.dumps(
        {
            "meta": {
                "tickers": clean_tickers,
                "period": period,
                "financial_limit": statement_limit,
                "news_limit": company_news_limit,
                "max_concurrency": concurrency,
            },
            "data": screen_data,
        },
        indent=2,
    )


@mcp.tool()
async def get_historical_stock_prices(
    ticker: str,
    start_date: str,
    end_date: str,
    interval: str = "day",
    interval_multiplier: int = 1,
) -> str:
    """Gets historical stock prices for a company.

    Args:
        ticker: Ticker symbol of the company (e.g. AAPL, GOOGL)
        start_date: Start date of the price data (e.g. 2020-01-01)
        end_date: End date of the price data (e.g. 2020-12-31)
        interval: Interval of the price data (e.g. minute, hour, day, week, month)
        interval_multiplier: Multiplier of the interval (e.g. 1, 2, 3)
    """
    # Fetch data from the API
    url = f"{FINANCIAL_DATASETS_API_BASE}/prices/?ticker={ticker}&interval={interval}&interval_multiplier={interval_multiplier}&start_date={start_date}&end_date={end_date}"
    data = await make_request(url)

    # Check if data is found
    if not data:
        return "Unable to fetch prices or no prices found."

    # Extract the prices
    prices = data.get("prices", [])

    # Check if prices are found
    if not prices:
        return "Unable to fetch prices or no prices found."

    # Stringify the prices
    return json.dumps(prices, indent=2)


@mcp.tool()
async def get_company_news(ticker: str) -> str:
    """Get news for a company.

    Args:
        ticker: Ticker symbol of the company (e.g. AAPL, GOOGL)
    """
    # Fetch data from the API
    url = f"{FINANCIAL_DATASETS_API_BASE}/news/?ticker={ticker}"
    data = await make_request(url)

    # Check if data is found
    if not data:
        return "Unable to fetch news or no news found."

    # Extract the news
    news = data.get("news", [])

    # Check if news are found
    if not news:
        return "Unable to fetch news or no news found."
    return json.dumps(news, indent=2)


@mcp.tool()
async def get_available_crypto_tickers() -> str:
    """
    Gets all available crypto tickers.
    """
    # Fetch data from the API
    url = f"{FINANCIAL_DATASETS_API_BASE}/crypto/prices/tickers"
    data = await make_request(url)

    # Check if data is found
    if not data:
        return "Unable to fetch available crypto tickers or no available crypto tickers found."

    # Extract the available crypto tickers
    tickers = data.get("tickers", [])

    # Stringify the available crypto tickers
    return json.dumps(tickers, indent=2)


@mcp.tool()
async def get_crypto_prices(
    ticker: str,
    start_date: str,
    end_date: str,
    interval: str = "day",
    interval_multiplier: int = 1,
) -> str:
    """
    Gets historical prices for a crypto currency.
    """
    # Fetch data from the API
    url = f"{FINANCIAL_DATASETS_API_BASE}/crypto/prices/?ticker={ticker}&interval={interval}&interval_multiplier={interval_multiplier}&start_date={start_date}&end_date={end_date}"
    data = await make_request(url)

    # Check if data is found
    if not data:
        return "Unable to fetch prices or no prices found."

    # Extract the prices
    prices = data.get("prices", [])

    # Check if prices are found
    if not prices:
        return "Unable to fetch prices or no prices found."

    # Stringify the prices
    return json.dumps(prices, indent=2)


@mcp.tool()
async def get_historical_crypto_prices(
    ticker: str,
    start_date: str,
    end_date: str,
    interval: str = "day",
    interval_multiplier: int = 1,
) -> str:
    """Gets historical prices for a crypto currency.

    Args:
        ticker: Ticker symbol of the crypto currency (e.g. BTC-USD). The list of available crypto tickers can be retrieved via the get_available_crypto_tickers tool.
        start_date: Start date of the price data (e.g. 2020-01-01)
        end_date: End date of the price data (e.g. 2020-12-31)
        interval: Interval of the price data (e.g. minute, hour, day, week, month)
        interval_multiplier: Multiplier of the interval (e.g. 1, 2, 3)
    """
    # Fetch data from the API
    url = f"{FINANCIAL_DATASETS_API_BASE}/crypto/prices/?ticker={ticker}&interval={interval}&interval_multiplier={interval_multiplier}&start_date={start_date}&end_date={end_date}"
    data = await make_request(url)

    # Check if data is found
    if not data:
        return "Unable to fetch prices or no prices found."

    # Extract the prices
    prices = data.get("prices", [])

    # Check if prices are found
    if not prices:
        return "Unable to fetch prices or no prices found."

    # Stringify the prices
    return json.dumps(prices, indent=2)


@mcp.tool()
async def get_current_crypto_price(ticker: str) -> str:
    """Get the current / latest price of a crypto currency.

    Args:
        ticker: Ticker symbol of the crypto currency (e.g. BTC-USD). The list of available crypto tickers can be retrieved via the get_available_crypto_tickers tool.
    """
    # Fetch data from the API
    url = f"{FINANCIAL_DATASETS_API_BASE}/crypto/prices/snapshot/?ticker={ticker}"
    data = await make_request(url)

    # Check if data is found
    if not data:
        return "Unable to fetch current price or no current price found."

    # Extract the current price
    snapshot = data.get("snapshot", {})

    # Check if current price is found
    if not snapshot:
        return "Unable to fetch current price or no current price found."

    # Stringify the current price
    return json.dumps(snapshot, indent=2)


@mcp.tool()
async def get_sec_filings(
    ticker: str,
    limit: int = 10,
    filing_type: str | None = None,
) -> str:
    """Get all SEC filings for a company.

    Args:
        ticker: Ticker symbol of the company (e.g. AAPL, GOOGL)
        limit: Number of SEC filings to return (default: 10)
        filing_type: Type of SEC filing (e.g. 10-K, 10-Q, 8-K)
    """
    # Fetch data from the API
    url = f"{FINANCIAL_DATASETS_API_BASE}/filings/?ticker={ticker}&limit={limit}"
    if filing_type:
        url += f"&filing_type={filing_type}"
 
    # Call the API
    data = await make_request(url)

    # Extract the SEC filings
    filings = data.get("filings", [])

    # Check if SEC filings are found
    if not filings:
        return f"Unable to fetch SEC filings or no SEC filings found."

    # Stringify the SEC filings
    return json.dumps(filings, indent=2)

if __name__ == "__main__":
    # Log server startup
    logger.info("Starting Financial Datasets MCP Server...")

    # Initialize and run the server
    mcp.run(transport="stdio")

    # This line won't be reached during normal operation
    logger.info("Server stopped")
