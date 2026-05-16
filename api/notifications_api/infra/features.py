from pydantic import BaseModel


class FeatureFlagsConfig(BaseModel):
    operational_tail: bool = True
    bc_eventual_mode: bool = True
    dlq_replay_noop: bool = False
