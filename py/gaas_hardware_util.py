import psutil
import GPUtil
import platform
from datetime import datetime
import re


# sourced from - https://www.thepythoncode.com/article/get-hardware-system-information-python
class HardwareUtil():

  def __init__(self):
    self.stats = {}
  
  def get_all(self):
    self.stats = {}
    self.get_storage()
    self.get_gpu()
    self.get_cpu()
    return self.stats
    
  # need to do for specific partition
  def get_storage(self):
    storage = {}
    partitions = psutil.disk_partitions()
    all_total = 0
    all_available = 0
    all_used = 0
    for partition in partitions:
      #print(f"=== Device: {partition.device} ===")
      #print(f"  Mountpoint: {partition.mountpoint}")
      #print(f"  File system type: {partition.fstype}")
      try:
        partition_usage = psutil.disk_usage(partition.mountpoint)
      except PermissionError:
        # this can be catched due to the disk that
        # isn't ready
        continue
      #print(f"  Total Size: {self.get_size(partition_usage.total)}")
      #print(f"  Used: {self.get_size(partition_usage.used)}")
      #print(f"  Free: {self.get_size(partition_usage.free)}")
      #print(f"  Percentage: {partition_usage.percent}%")
      all_total = all_total + partition_usage.total
      all_used = all_used + partition_usage.used
      all_available = all_available + partition_usage.free
      # get IO statistics since boot
    #disk_io = psutil.disk_io_counters()
    #print(f"Total read: {self.get_size(disk_io.read_bytes)}")
    #print(f"Total write: {self.get_size(disk_io.write_bytes)}")    
    storage = {"total":all_total, "used": all_used, "available": all_available}
    self.stats.update({"storage": storage})
    return storage
    
  def get_gpu(self): 
    gpus = GPUtil.getGPUs()
    list_gpus = []
    all_total = 0
    all_used = 0
    all_available = 0
    gpu_data = {}
    for gpu in gpus:
      # get the GPU id
      gpu_id = gpu.id
      # name of GPU
      gpu_name = gpu.name
      # get % percentage of GPU usage of that GPU
      #gpu_load = f"{gpu.load*100}%"
      # get free memory in MB format
      gpu_free_memory = gpu.memoryFree
      # get used memory
      gpu_used_memory = gpu.memoryUsed
      # get total memory
      gpu_total_memory = gpu.memoryTotal
      # get GPU temperature in Celsius
      gpu_temperature = f"{gpu.temperature} °C"
      gpu_uuid = gpu.uuid
      
      # add all details
      all_total = all_total + gpu_total_memory
      all_used = all_used + gpu_used_memory
      all_available = all_available + gpu_free_memory
      
      #list_gpus.append((
      #    gpu_id, gpu_name, gpu_load, gpu_free_memory, gpu_used_memory,
      #    gpu_total_memory, gpu_temperature, gpu_uuid
      #))
      gpu_details = {"name":gpu_name,  "total": gpu_total_memory, "available":gpu_free_memory, "used":gpu_used_memory, "temperature":gpu_temperature}
      gpu_data.update({gpu_id:gpu_details})
    
    # sort the info (highest to lowest)
    gpu_data = {'gpus': {k: v for k, v in sorted(gpu_data.items(), key = lambda item: item[1]['available'], reverse= True)}}

    gpu_data.update({"total":all_total})
    gpu_data.update({"available":all_available})
    gpu_data.update({"used":all_used})
    
    self.stats.update({"gpu": gpu_data})
    
    return gpu
  
  def get_cpu(self):
    # let's print CPU information
    cpu = {}
    
    #print("="*40, "CPU Info", "="*40)
    # number of cores
    cpu.update({"physical_cores":psutil.cpu_count(logical=False)})
    cpu.update({"total_cores":psutil.cpu_count(logical=True)})
    #print("Physical cores:", psutil.cpu_count(logical=False))
    #print("Total cores:", psutil.cpu_count(logical=True))
    # CPU frequencies
    #cpufreq = psutil.cpu_freq()
    #print(f"Max Frequency: {cpufreq.max:.2f}Mhz")
    #print(f"Min Frequency: {cpufreq.min:.2f}Mhz")
    #print(f"Current Frequency: {cpufreq.current:.2f}Mhz")
    # CPU usage
    #print("CPU Usage Per Core:")
    for i, percentage in enumerate(psutil.cpu_percent(percpu=True, interval=1)):
      core = f"core_{i}"
      cpu.update({core:percentage})
      print(f"Core {i}: {percentage}%")
    print(f"Total CPU Usage: {psutil.cpu_percent()}%")
    self.stats.update({"cpu": cpu})
    
 
  
  def convert_to_bytes(self, bytes, suffix="B"):
    factor = 1024
    units = ["B", "K", "M", "G", "T", "P"]
    if suffix in units:
      multiplier = units.index(suffix)
      value = bytes * (1024**multiplier)
      return value
    else:
      return bytes
      
  def parse_to_bytes(self, readable_value):
    numeric_value = float(re.findall(r"[-+]?(?:\d*\.*\d+)", readable_value)[0])
    alpha_value = ''.join(x for x in readable_value if x.isalpha())[0]
    return self.convert_to_bytes(numeric_value, alpha_value)
  
  def get_size(self, val, suffix="B"):
    """
    Scale bytes to its proper format
    e.g:
        1253656 => '1.20MB'
        1253656678 => '1.17GB'
    """
    factor = 1024
    for unit in ["", "K", "M", "G", "T", "P"]:
        if val < factor:
            return f"{val:.2f}{unit}{suffix}"
        val /= factor

  def find_available_gpus(self, memory_needed):
    self.get_gpu()
    
    gpu_to_allocate = []
    if (memory_needed >= self.stats['gpu']['available']):
        return gpu_to_allocate
    
    for k,v in self.stats['gpu']['gpus'].items():
        memory_needed -= v['available']
        gpu_to_allocate.append(k)
        if (memory_needed < 0):
            break

    return gpu_to_allocate
