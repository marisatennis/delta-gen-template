"""Pretty-print helpers for SharePoint list ingestion."""


def format_results(results, show_details=True):
    """Return a human-friendly multiline summary for list ingestion results."""
    if not isinstance(results, list):
        return str(results)

    lines = []
    total_processed = sum(int(r.get("items_processed", 0) or 0) for r in results)
    total_skipped = sum(int(r.get("items_skipped", 0) or 0) for r in results)
    load_types = sorted({r.get("load_type", "unknown") for r in results})
    write_modes = sorted({r.get("write_mode", "unknown") for r in results})

    lines.append(f"Lists run: {len(results)}")
    lines.append(f"Items processed: {total_processed}, skipped: {total_skipped}")
    lines.append(f"Load types: {', '.join(load_types)}")
    lines.append(f"Write modes: {', '.join(write_modes)}")
    lines.append("")

    if show_details:
        rows = [("List", "Processed", "Skipped", "Load", "Write")]
        for res in results:
            rows.append((
                res.get("list_name", "unknown"),
                str(res.get("items_processed", 0) or 0),
                str(res.get("items_skipped", 0) or 0),
                res.get("load_type", "unknown"),
                res.get("write_mode", "unknown"),
            ))

        col_widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
        fmt = "  ".join("{:<%d}" % w for w in col_widths)
        lines.append(fmt.format(*rows[0]))
        lines.append("-" * (sum(col_widths) + 2 * (len(col_widths) - 1)))
        for r in rows[1:]:
            lines.append(fmt.format(*r))

    return "\n".join(lines)


def print_parallel_results(results, show_details=True):
    """Pretty-print SharePoint list ingestion results to console."""
    print(format_results(results, show_details=show_details))
