"""SharePoint list ingestion helpers."""

def _safe_create_dataframe(spark, records):
    """Create a DataFrame from records, returning None if the data is empty
    or Spark cannot infer the schema (e.g. all-null columns)."""
    if not records:
        return None
    try:
        return spark.createDataFrame(records)
    except Exception as e:
        if "CANNOT_DETERMINE_TYPE" in str(e):
            print(f"Skipping batch -- Spark cannot infer schema (likely empty data): {e}")
            return None
        raise


def fetch_sharepoint_list_items(
    access_token,
    list_url,
    *,
    page_size=None,
    max_pages=None,
    timeout=60,
):
    """
    Fetches list items from a SharePoint API endpoint (Graph or SharePoint REST).

    Parameters:
    -----------
    access_token : str
        OAuth bearer token
    list_url : str
        Full API URL for list items.
        Graph example:
        https://graph.microsoft.com/v1.0/sites/{site-id}/lists/{list-id}/items?expand=fields
        SharePoint REST example:
        https://{tenant}.sharepoint.com/sites/{site}/_api/web/lists/getbytitle('List')/items
    page_size : int, optional
        Max items per page (Graph uses $top). None to use API default.
    max_pages : int, optional
        Hard limit on pages to fetch (useful for testing).
    timeout : int
        Requests timeout in seconds.

    Returns:
    --------
    list
        List of list item payloads as dicts.
    """
    import requests

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    url = list_url
    if page_size is not None and "$top=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}$top={page_size}"

    items = []
    pages = 0

    while url:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        data = response.json()

        if "value" in data:
            items.extend(data.get("value", []))
            url = data.get("@odata.nextLink")
        elif "d" in data:
            payload = data.get("d", {})
            items.extend(payload.get("results", []))
            url = payload.get("__next")
        else:
            break

        pages += 1
        if max_pages is not None and pages >= max_pages:
            break

    return items


def build_delta_url(list_items_url):
    """
    Converts a Graph list items URL to a delta URL.

    Parameters:
    -----------
    list_items_url : str
        Graph list items URL (with expand=fields)

    Returns:
    --------
    str
        Delta URL for the list
    """
    if "/items/delta" in list_items_url:
        return list_items_url
    if "/items?" in list_items_url:
        return list_items_url.replace("/items?", "/items/delta?")
    if "/items" in list_items_url:
        return list_items_url.replace("/items", "/items/delta")
    raise ValueError("Invalid list items URL for delta conversion.")


def fetch_sharepoint_list_delta_items(
    access_token,
    delta_url,
    *,
    delta_link=None,
    page_size=None,
    timeout=60,
):
    """
    Fetches incremental list changes via Microsoft Graph delta query.

    Parameters:
    -----------
    access_token : str
        OAuth bearer token
    delta_url : str
        Delta endpoint for list items (e.g., .../items/delta?expand=fields)
    delta_link : str, optional
        Stored deltaLink from previous run (if present, resumes from that checkpoint)
    page_size : int, optional
        Max items per page (Graph uses $top). None to use API default.
    timeout : int
        Requests timeout in seconds.

    Returns:
    --------
    tuple
        (items, new_delta_link)
    """
    import requests

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    url = delta_link or delta_url
    if page_size is not None and "$top=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}$top={page_size}"

    items = []
    new_delta_link = None

    while url:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        data = response.json()

        items.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
        new_delta_link = data.get("@odata.deltaLink", new_delta_link)

        if not url:
            break

    return items, new_delta_link


def resolve_graph_list_items_url(access_token, site_url, list_name):
    """
    Resolves a Microsoft Graph list items URL from a SharePoint site URL and list name.

    Parameters:
    -----------
    access_token : str
        OAuth bearer token
    site_url : str
        SharePoint site URL (e.g., 'https://tenant.sharepoint.com/sites/MySite')
    list_name : str
        SharePoint list display name

    Returns:
    --------
    str
        Graph list items URL with fields expanded
    """
    import requests
    from urllib.parse import urlparse

    parsed = urlparse(site_url)
    hostname = parsed.hostname
    site_path = parsed.path

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    site_lookup_url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}"
    site_response = requests.get(site_lookup_url, headers=headers, timeout=30)
    site_response.raise_for_status()
    site_id = site_response.json().get("id")

    lists_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists?$filter=displayName eq '{list_name}'"
    lists_response = requests.get(lists_url, headers=headers, timeout=30)
    lists_response.raise_for_status()
    lists = lists_response.json().get("value", [])
    if not lists:
        raise ValueError(f"List not found: {list_name}")

    list_id = lists[0].get("id")
    return f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?expand=fields"


def normalize_delta_items(items, fields_key="fields"):
    """
    Flattens Graph delta payloads to row dicts with soft delete markers.

    Parameters:
    -----------
    items : list
        Delta list item payloads
    fields_key : str
        Key containing SharePoint column values (Graph uses 'fields')

    Returns:
    --------
    list
        Flattened row dictionaries with is_deleted flag
    """
    normalized = []
    for item in items:
        removed = item.get("@removed")
        if removed is not None:
            normalized.append({
                "id": item.get("id"),
                "is_deleted": True,
                "removed_reason": removed.get("reason"),
            })
            continue

        fields = item.get(fields_key)
        if fields:
            record = dict(fields)
            record["id"] = item.get("id")
            record["webUrl"] = item.get("webUrl")
            record["is_deleted"] = False
            normalized.append(record)
        else:
            record = dict(item)
            record["is_deleted"] = False
            normalized.append(record)
    return normalized


def normalize_list_items(items, fields_key="fields"):
    """
    Flattens Graph list item payloads to row dicts.

    Parameters:
    -----------
    items : list
        Raw list item payloads
    fields_key : str
        Key containing SharePoint column values (Graph uses 'fields')

    Returns:
    --------
    list
        Flattened row dictionaries
    """
    normalized = []
    for item in items:
        fields = item.get(fields_key)
        if fields:
            record = dict(fields)
            record["id"] = item.get("id")
            record["webUrl"] = item.get("webUrl")
            normalized.append(record)
        else:
            normalized.append(item)
    return normalized


def run_ingestion(
    spark,
    access_token,
    site_url,
    list_name,
    target_table,
    *,
    incremental=True,
    delta_state_table=None,
    page_size=None,
    write_mode="append",
):
    """
    Runs SharePoint list ingestion (full or incremental delta).

    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
    access_token : str
        OAuth bearer token
    site_url : str
        SharePoint site URL
    list_name : str
        SharePoint list display name
    target_table : str
        Lakehouse target table name
    incremental : bool
        True for delta incremental loads, False for full list pull
    delta_state_table : str, optional
        Delta state table for incremental loads
    page_size : int, optional
        Page size for API calls
    write_mode : str
        Delta write mode (append, overwrite)

    Returns:
    --------
    dict
        Summary of ingestion results
    """
    from datetime import datetime
    from .storage import write_dataframe_to_lakehouse

    results = {
        "list_name": list_name,
        "items_processed": 0,
        "items_skipped": 0,
        "write_mode": write_mode,
        "load_type": "incremental" if incremental else "full",
    }

    list_items_url = resolve_graph_list_items_url(access_token, site_url, list_name)

    if not incremental:
        items = fetch_sharepoint_list_items(
            access_token, list_items_url, page_size=page_size
        )
        records = normalize_list_items(items)
        if not records:
            results["items_skipped"] = 1
            return results

        for record in records:
            record["is_deleted"] = False

        df = _safe_create_dataframe(spark, records)
        if df is None:
            results["items_skipped"] = 1
            return results
        write_dataframe_to_lakehouse(df, target_table, mode=write_mode)
        results["items_processed"] = df.count()
        return results

    if not delta_state_table:
        raise ValueError("delta_state_table is required for incremental loads")

    from .metadata import ensure_delta_table, get_latest_delta_link, update_delta_link

    ensure_delta_table(spark, delta_state_table)

    delta_url = build_delta_url(list_items_url)
    delta_link = get_latest_delta_link(spark, site_url, list_name, delta_state_table)
    items, new_delta_link = fetch_sharepoint_list_delta_items(
        access_token, delta_url, delta_link=delta_link, page_size=page_size
    )
    records = normalize_delta_items(items)
    if not records:
        results["items_skipped"] = 1
        if new_delta_link:
            update_delta_link(
                spark,
                site_url,
                list_name,
                new_delta_link,
                delta_state_table,
                updated_at=datetime.utcnow(),
            )
        return results

    df = _safe_create_dataframe(spark, records)
    if df is None:
        results["items_skipped"] = 1
        if new_delta_link:
            update_delta_link(
                spark, site_url, list_name, new_delta_link,
                delta_state_table, updated_at=datetime.utcnow(),
            )
        return results
    write_dataframe_to_lakehouse(df, target_table, mode=write_mode)
    results["items_processed"] = df.count()

    if new_delta_link:
        update_delta_link(
            spark,
            site_url,
            list_name,
            new_delta_link,
            delta_state_table,
            updated_at=datetime.utcnow(),
        )
    return results

def list_items_to_dataframe(spark, items, fields_key="fields"):
    """
    Converts list item payloads to a Spark DataFrame.

    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
    items : list
        Raw list item payloads
    fields_key : str
        Key containing SharePoint column values (Graph uses 'fields')

    Returns:
    --------
    DataFrame or None
        Spark DataFrame if items exist, else None
    """
    if not items:
        return None
    records = normalize_list_items(items, fields_key=fields_key)
    return _safe_create_dataframe(spark, records)
