from processors import acc, gyr, ain, fft, data

def get_processor(topic):
    if topic == 'acc': return acc
    if topic == 'gyr': return gyr
    if topic == 'ain': return ain
    if topic == 'fft': return fft
    if topic == 'data': return data
    return None