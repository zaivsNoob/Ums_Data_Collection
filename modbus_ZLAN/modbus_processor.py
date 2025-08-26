from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian

class ModbusDataProcessor:
    @staticmethod
    def convert_int16_to_32_float(registers, byteorder=Endian.Big, wordorder=Endian.Big):
        decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
        return decoder.decode_32bit_float()

    @staticmethod
    def convert_int16_to_64_float(registers, byteorder=Endian.Big, wordorder=Endian.Big):
        decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)
        return decoder.decode_64bit_float()
