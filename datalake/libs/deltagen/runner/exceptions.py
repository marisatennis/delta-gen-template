"""Exceptions for the runner module."""
from __future__ import annotations


class PlanBuilderError(Exception):
    """Exception raised when PlanBuilder encounters an error."""

    def __init__(
        self,
        message: str,
        stage: str | None = None,
        column: str | None = None,
        detail: str | None = None,
    ):
        self.stage = stage
        self.column = column
        self.detail = detail

        full_message = f"PlanBuilderError: {message}"
        if stage:
            full_message += f"\n  Stage: {stage}"
        if column:
            full_message += f"\n  Column: {column}"
        if detail:
            full_message += f"\n  Detail: {detail}"

        super().__init__(full_message)


class WriterError(Exception):
    """Exception raised when Writer encounters an error."""

    def __init__(
        self,
        message: str,
        target: str | None = None,
        mode: str | None = None,
        detail: str | None = None,
    ):
        self.target = target
        self.mode = mode
        self.detail = detail

        full_message = f"WriterError: {message}"
        if target:
            full_message += f"\n  Target: {target}"
        if mode:
            full_message += f"\n  Mode: {mode}"
        if detail:
            full_message += f"\n  Detail: {detail}"

        super().__init__(full_message)
