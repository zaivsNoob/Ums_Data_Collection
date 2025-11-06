import asyncio
import traceback
from pymodbus.client import AsyncModbusTcpClient
from datetime import datetime

from dotenv import load_dotenv
import os

from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian

import struct

#==========================================================Conversion Functions==========================================================
log_file_path = os.getenv('LOG_FILE_PATH')
def log_message(message):
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"{datetime.now()}: {message}\n")

def convertRegistersToDataM1M20_4(register1, register2, register3, register4, resolution):
    combined_value = int(f"{register1:04x}{register2:04x}{register3:04x}{register4:04x}", 16)
    return round(combined_value * resolution, 3)

def convertRegistersToDataM1M20_2(register1, register2, resolution):
    combined_value = int((hex(register1) + hex(register2)[2:]), 16)
    return round(combined_value * resolution,3)

def convert_u16_to_int16(registers, byteorder=Endian.Big, wordorder=Endian.Big):
    decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
    return decoder.decode_16bit_int()

def convert_int16_to_64_float(registers, byteorder=Endian.Big, wordorder=Endian.Big):
    decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
    return decoder.decode_64bit_float()

def convert_u16_to_16_int(registers, byteorder=Endian.Big, wordorder=Endian.Big):
    decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
    return decoder.decode_16bit_int()

def convert_u16_to_16_uint(registers, byteorder=Endian.Big, wordorder=Endian.Big):
    decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
    return decoder.decode_16bit_uint

def convert_u16_to_16_float(registers, byteorder=Endian.Big, wordorder=Endian.Big):
    decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
    return decoder.decode_16bit_float()

def convert_u16_to_32_int(registers, byteorder=Endian.Big, wordorder=Endian.Big):
    decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
    return decoder.decode_32bit_int()

def convert_u16_to_32_uint(registers, byteorder=Endian.Big, wordorder=Endian.Big):
    decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
    return decoder.decode_32bit_uint()

def convert_u16_to_32_float(registers, byteorder=Endian.Big, wordorder=Endian.Big):
    decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
    return decoder.decode_32bit_float()

def convert_u16_to_64_int(registers, byteorder=Endian.Big, wordorder=Endian.Big):
    decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
    return decoder.decode_64bit_int()

def convert_u16_to_64_uint(registers, byteorder=Endian.Big, wordorder=Endian.Big):
    decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
    return decoder.decode_64bit_uint()

def convert_u16_to_64_float(registers, byteorder=Endian.Big, wordorder=Endian.Big):
    decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
    return decoder.decode_64bit_float()







def convert_int16_to_32_float(registers, byteorder='little'):
    try:
        # Check if at least two registers are provided
        if len(registers) < 2:
            raise ValueError("At least two registers are required for 32-bit float conversion")
        
        # Convert 16-bit registers to bytes in little-endian order
        byte_data = bytes([
            (registers[0] & 0xFF),          # Low byte of first register
            ((registers[0] >> 8) & 0xFF),   # High byte of first register
            (registers[1] & 0xFF),          # Low byte of second register
            ((registers[1] >> 8) & 0xFF)    # High byte of second register
        ])
        
        return struct.unpack('<f', byte_data)[0]
    except Exception as e:
        log_message(f"Error converting registers to 32-bit float: {traceback.format_exc()}")
        return None





#==========================================================Device Specific Processing Functions==========================================================

def process7KTMeter(registers, offset):
    try:
        data_entry = {}

        for data_point in range(10):
            start_index = data_point  * 2 + offset * 20
            end_index = start_index + 2

            data_entry[f"data_{data_point + 1}"] =convert_int16_to_32_float(registers[start_index:end_index])
        return data_entry
    except Exception as e:
        log_message(f"Error processing 7KT Meter registers: {traceback.format_exc()}")
        return {}    

def processPAC3120(registers, offset):
    try:
        data_entry = {}

        for j in range(2):
            start_index = j * 4 + offset * 40
            end_index = start_index + 4
            data_entry[f"data_{j + 1}"] = convert_int16_to_64_float(registers[start_index:end_index])
        data_entry['data_1']=(data_entry['data_1']-data_entry['data_2'])/1000

        for i in range(4, 20):
            start_index = i * 2 + offset * 40
            end_index = start_index + 2
            data_entry[f"data_{i - 2}"] = convert_u16_to_32_float(registers[start_index:end_index])            

        data_entry['data_8']=data_entry['data_8']/1000
        data_entry['data_18']=0
        data_entry['data_19']=0
        data_entry['data_20']=0
        return data_entry 
    except Exception as e:
        log_message(f"Error processing PAC3120 registers: {traceback.format_exc()}")
        return {}

def processMFM384And7kt(registers, offset):
    try:
        data_entry = {}

        for j in range(20):
            start_index = j * 2 + offset * 40
            end_index = start_index + 2
            data_entry[f"data_{j + 1}"] = convert_u16_to_32_float(registers[start_index:end_index])
        data_entry['data_1']=data_entry['data_1']-data_entry['data_2']

        for i in range (2, 20):
            data_entry[f'data_{i}']=data_entry[f'data_{i+1}']
        data_entry['data_20']=0


        return data_entry
    except Exception as e:
        log_message(f"Error processing MFM-384 registers: {traceback.format_exc()}")
        return {}
       
def processFlowMeter(registers, offset, flow_convert_type, volume_convert_type):
    try:
        flow_converter, volume_converter= waterSteamDataConverter(flow_convert_type, volume_convert_type)
        if not flow_converter or not volume_converter:
            log_message(f"Invalid converter type: flow_convert_type={flow_convert_type}, volume_convert_type={volume_convert_type}")
            return {}
        data_entry = {}
        for j in range(2):
            start_index = j * 2 + offset * 40
            end_index = start_index + 2
            if j==0:
                data_entry[f"data_{j + 1}"] = flow_converter(registers[start_index:end_index])
            else:
                data_entry[f"data_{j + 1}"] = volume_converter(registers[start_index:end_index])
        
        for i in range (3, 21):
            data_entry[f'data_{i}']=0

        return data_entry
    except Exception as e:
        log_message(f"Error processing Flow Meter registers: {traceback.format_exc()}")
        return {}

def waterSteamDataConverter(flow_convert_type, volume_convert_type):
    try:
        converter={
        'uint16_be':convert_u16_to_16_uint,
        'int16_be':convert_u16_to_16_int,
        'float16_be':convert_u16_to_16_float,
        'uint32_be':convert_u16_to_32_uint,
        'int32_be':convert_u16_to_32_int,
        'float32_be':convert_u16_to_32_float,
        'uint64_be':convert_u16_to_64_uint,
        'int64_be':convert_u16_to_64_int,
        'float64_be':convert_u16_to_64_float,
        }
        return converter.get(flow_convert_type), converter.get(volume_convert_type)
    except Exception as e:
        log_message(f"Error in waterSteamDataConverter: {traceback.format_exc()}")
        return {}     


def processPLC(registers, offset):
        try:
            data_entry = {}

            for data_point in range(20):
                start_index = data_point  * 2 + offset * 40
                end_index = start_index + 2

                data_entry[f"data_{data_point + 1}"] =convert_u16_to_32_float(registers[start_index:end_index])

            return data_entry
        except Exception as e:
            log_message(f"Error processing PAC3120 registers: {traceback.format_exc()}")
            return {}
             
def processaq_hum_temp(registers, offset):
        try:
            data_entry = {}

            for data_point in range(20):
                start_index = data_point + offset * 40
                end_index = start_index + 1

                data_entry[f"data_{data_point + 1}"] =convert_u16_to_int16(registers[start_index:end_index])
            data_entry['data_1']=data_entry['data_1']/10
            data_entry['data_2']=data_entry['data_2']/10
            data_entry['data_3']=data_entry['data_3'] if data_entry['data_3']>=0 else 0
                # data_entry[f"data_{data_point + 1}"] =convert_u16_to_32_float(registers[start_index:end_index])

            return data_entry
        except Exception as e:
            log_message(f"Error processing PAC3120 registers: {traceback.format_exc()}")
            return {}

    