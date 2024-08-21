from datetime import timedelta, datetime

def generate_date_range(start_date: str, end_date: str) -> list[str]:
    """
    Generate a list of dates between two given dates in 'YYYYMMDD' format.

    Args:
        start_date (str): The start date in 'YYYYMMDD' format.
        end_date (str): The end date in 'YYYYMMDD' format.

    Returns:
        list[str]: A list of dates in 'YYYYMMDD' format.

    Raises:
        ValueError: If the end date is earlier than the start date.
    """
    start = datetime.strptime(start_date, "%Y%m%d").date()
    end = datetime.strptime(end_date, "%Y%m%d").date()
    num_days = (end - start).days

    if num_days < 0:
        raise ValueError("End date must be greater or equal than to start date.")
    return [(start + timedelta(days=i)).strftime("%Y%m%d") for i in range(num_days + 1)]
