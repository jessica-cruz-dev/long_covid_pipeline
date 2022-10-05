""" """
from collections import namedtuple


class JobmonConstants:
    R_EXE = (
        '/path/to/execRscript.sh -i /path/to/singularity-image.img -s'
    )
    MAX_ATTEMPTS = 5


class LongSaveConstants:
    MEASURES_LONG = namedtuple(
        'cognitive_mild',
        'cognitive_severe',
        'fatigue',
        'respiratory_mild',
        'respiratory_moderate',
        'respiratory_severe',
        'cognitive_mild_fatigue',
        'cognitive_severe_fatigue',
        'cognitive_mild_respiratory_mild',
        'cognitive_mild_respiratory_moderate',
        'cognitive_mild_respiratory_severe',
        'cognitive_severe_respiratory_mild',
        'cognitive_severe_respiratory_moderate',
        'cognitive_severe_respiratory_severe',
        'fatigue_respiratory_mild',
        'fatigue_respiratory_moderate',
        'fatigue_respiratory_severe',
        'cognitive_mild_fatigue_respiratory_mild',
        'cognitive_mild_fatigue_respiratory_moderate',
        'cognitive_mild_fatigue_respiratory_severe',
        'cognitive_severe_fatigue_respiratory_mild',
        'cognitive_severe_fatigue_respiratory_moderate',
        'cognitive_severe_fatigue_respiratory_severe',
        'any',
        'midmod',
        'hospital',
        'icu',
        'gbs'
    )


class ShortSaveConstants:
    MEASURES_SHORT = namedtuple('asymp', 'mild', 'moderate', 'hospital', 'icu')


class PipelineConstants:
    DEAFULT_QUEUE = 'queue.q'
