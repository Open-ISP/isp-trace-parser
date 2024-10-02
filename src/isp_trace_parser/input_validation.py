from pathlib import Path


from typeguard import typechecked


def input_directory(path: Path) -> None:
    if is_valid_path(path):
        if not path.is_dir():
            raise ValueError(f"Directory {path} does not exist")
    else:
        raise ValueError(f"Invalid input directory path: {path}")


def parsed_directory(path: Path) -> None:
    if not is_valid_path(path):
        raise ValueError(f"Invalid parsed directory path: {path}")


def is_valid_path(path: str | Path) -> bool:
    try:
        Path(path)
        return True
    except (TypeError, ValueError):
        return False


@typechecked
def try_typeguard(word: str) -> int:
    return len(word)
