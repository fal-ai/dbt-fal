from __future__ import annotations


def main_check(output):
    output += f"inner name: {__name__}\n"
    return output
