from pathlib import Path


def input_directory(path: Path | str) -> Path:
    path = is_valid_path(path)
    if not path.is_dir():
        msg = f"Directory {path} does not exist"
        raise ValueError(msg)
    return path


def parsed_directory(path: str | Path) -> Path:
    return is_valid_path(path)


def is_valid_path(path: str | Path) -> Path:
    try:
        return Path(path)
    except (TypeError, ValueError):
        msg = f"Invalid parsed directory path: {path}"
        raise ValueError(msg) from None


def start_year_before_end_year(start_year, end_year) -> None:
    if end_year < start_year:
        msg = f"Start year {end_year} < end year {start_year}"
        raise ValueError(msg)
