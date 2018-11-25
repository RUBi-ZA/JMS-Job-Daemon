from .torque_2_4_x import Torque

# Register schedulers by adding them to the following dictionary
schedulers = {
    'torque_2_4_x': {
        'name': 'Torque v2.4.x',
        'scheduler': Torque
    }
}
