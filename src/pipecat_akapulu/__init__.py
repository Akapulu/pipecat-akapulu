"""Pipecat processor for an Akapulu avatar."""

from pipecat_akapulu.avatar import AkapuluAvatar
from pipecat_akapulu.call import AVATAR_NAME, BROWSER_NAME, create_akapulu_call
from pipecat_akapulu.daily import akapulu_daily_params

__all__ = [
    "AVATAR_NAME",
    "BROWSER_NAME",
    "AkapuluAvatar",
    "akapulu_daily_params",
    "create_akapulu_call",
]
