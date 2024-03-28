# Error message
# contributors: smlee

# History
# 2024-03-15 | v1.0 - first commit

# Main
def emsg(e:object) -> str:
    """Custom error message

    Args:
        e: e from Exception
    """

    msg = f"{type(e).__name__}, {e.args}, LINE AT: {e.__traceback__.tb_lineno}"

    return msg