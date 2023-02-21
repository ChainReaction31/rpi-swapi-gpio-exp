import time
from datetime import datetime

import adafruit_dht as DHT
import board
import psutil
from oshdatacore.component_implementations import (DataRecordComponent,
                                                   QuantityComponent,
                                                   TimeComponent)
from oshdatacore.encoding import TextEncoding
from pyswapi.datastreams_and_observations import Datastream, ObservationFormat
from pyswapi.system import System


def create_system():
    new_sys = System(name='RPi Temp Sensor',
                     uid='urn:cr31:RPi:temp2',
                     definition='www.test.org/rpi/temp',
                     description='Simple Sensor providing Temperature and Humidity Measurements',
                     def_type='www.test.org/rpi/temp',
                     node_url='http://192.168.1.137',
                     node_port='8181',
                     node_endpoint='sensorhub')

    new_sys.insert_system()
    return new_sys


def create_output():
    root = DataRecordComponent(name='rpi-temp',
                               label='RPi Temperature and Humidity',
                               definition='')

    return root


def create_datastream(system, root_comp):
    ds = Datastream(name='Temperature & Humidity Datastream',
                    output_name='temp_ds',
                    parent_system=system,
                    obs_format=ObservationFormat.JSON.value,
                    encoding=TextEncoding(),
                    root_component=root_comp)
    return ds


if __name__ == '__main__':
    print('Hello world')
    system = create_system()
    root_dc = create_output()
    time_comp = TimeComponent(name='timestamp',
                              label='Timestamp'
                              )
    temp_comp = QuantityComponent(name='temperature',
                                  label='Temperature *C',
                                  uom='Cel',
                                  definition='')
    hum_comp = QuantityComponent(name='humidity',
                                 label='Humidity %',
                                 uom='%',
                                 definition='')
    root_dc.add_field(time_comp)
    root_dc.add_field(temp_comp)
    root_dc.add_field(hum_comp)
    ds = create_datastream(system, root_dc)
    ds.insert_datastream()

    for proc in psutil.process_iter():
        if proc.name() == 'libgpiod_pulsein' or proc.name() == 'libgpiod_pulsei':
            proc.kill()

    sensor = DHT.DHT11(board.D17)
    temp: float
    humidity: float
    while True:
        try:
            temp = sensor.temperature
            humidity = sensor.humidity
            print(f'Temp: {temp} *C \n Humidity: {humidity}%')
            ds.add_value_by_uuid(time_comp.get_uuid(),
                                 datetime.now().timestamp() * 1000)
            ds.add_value_by_uuid(temp_comp.get_uuid(), temp)
            ds.add_value_by_uuid(hum_comp.get_uuid(), humidity)
            ds.create_observation_from_current()
            ds.send_earliest_observation()
        except RuntimeError as error:
            print(error.args[0])
            time.sleep(3.0)
            continue
        except Exception as error:
            sensor.exit()
            raise error

        time.sleep(3.0)
