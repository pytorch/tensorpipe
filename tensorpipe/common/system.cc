#include <tensorpipe/common/system.h>

#include <cstring>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <thread>

namespace tensorpipe {

std::string tstampToStr(TimeStamp ts) {
  if (ts == kInvalidTimeStamp) {
    return "NA";
  }
  // print timestaps in microseconds.
  constexpr TimeStamp kDiv = 1000u;
  std::stringstream ss;
  ss << std::setw(9) << std::setfill(' ') << ts / kDiv;
  ss << "." << std::setw(3) << std::setfill('0') << ts % kDiv << "us";
  return ss.str();
}

optional<std::string> getProcFsStr(const std::string& file_name, pid_t tid) {
  std::ostringstream oss;
  oss << "/proc/" << tid << "/" << file_name;
  std::ifstream f{oss.str()};
  if (!f.is_open()) {
    return nullopt;
  }
  std::string v;
  getline(f, v);
  f.close();
  return v;
}

std::string removeBlankSpaces(std::string s) {
  // Remove blanks.
  s.erase(
      std::remove_if(
          s.begin(), s.end(), [](unsigned char c) { return std::isspace(c); }),
      s.end());
  return s;
}

/// Parse a CPU range or a single CPU id.
/// CPU_RANGE := <unsigned> | <unsigned>-<unsigned>
cpu_set_t parseCpusRange(std::string cpu_range_str) {
  cpu_range_str = removeBlankSpaces(cpu_range_str);

  cpu_set_t cpus;
  CPU_ZERO(&cpus);

  if (cpu_range_str.size() == 0) {
    return cpus;
  }

  std::istringstream s(cpu_range_str);
  std::string cpu_str;

  // Get first CPU in range (or one CPU).
  std::getline(s, cpu_str, '-');

  unsigned long cpu_start = std::stoul(cpu_str);
  if (s.eof()) {
    // No range, just one CPU.
    CPU_SET(cpu_start, &cpus);
    return cpus;
  }

  // Get second CPU in range.
  std::getline(s, cpu_str);
  unsigned long cpu_end = std::stoul(cpu_str);
  if (cpu_end <= cpu_start) {
    TP_THROW_EINVAL() << "Ill-formed CPU range. "
                      << "End CPU id must be larger than start CPU id. "
                      << "String: \"" << cpu_range_str << "\"";
  }

  for (unsigned long c = cpu_start; c <= cpu_end; ++c) {
    CPU_SET(c, &cpus);
  }
  return cpus;
}

cpu_set_t parseCpusList(std::string cpus_str) {
  cpu_set_t cpus;
  CPU_ZERO(&cpus);

  cpus_str = removeBlankSpaces(cpus_str);
  std::istringstream cpus_stream(cpus_str);

  std::string cpu_range_str;
  do {
    std::getline(cpus_stream, cpu_range_str, ',');
    cpu_set_t new_cpus = parseCpusRange(cpu_range_str);
    CPU_OR(&cpus, &new_cpus, &cpus);
  } while (!cpus_stream.eof());
  return cpus;
}

// Parse a text file with one line of text that is a comma-separated
// list of CPU ranges. E.g. 0,1-2,10-11,14.
cpu_set_t parseCpuLineFile(const std::string& path) {
  // Open file.
  std::ifstream s;
  s.open(path, std::ios::in);
  if (!s.is_open()) {
    TP_THROW_SYSTEM(ENFILE) << "File \"" << path << "\" not found";
  }

  // Parse line of CPUs. Expect only one line.
  std::string cpu_list;
  std::getline(s, cpu_list);
  if (!s.eof()) {
    std::string next_line;
    std::getline(s, next_line);
    if (next_line.size() > 0) {
      s.close();
      TP_THROW_SYSTEM(EINVAL)
          << "File " << path << " must contain only one line. "
          << "Superfluous content starts at: \"" << next_line << "\"";
    }
  }
  s.close();
  return parseCpusList(cpu_list);
}

/// Parse online and offline CPU IDs from /proc/fs
CpuSet makeCpuSet(optional<cpu_set_t> cpus) {
  auto online_cpus = parseCpuLineFile("/sys/devices/system/cpu/online");
  auto offline_cpus = parseCpuLineFile("/sys/devices/system/cpu/offline");

  if (CPU_COUNT(&online_cpus) == 0) {
    TP_THROW_SYSTEM(EPERM) << "At least one CPU must be online";
  }
  if (CPU_COUNT(&offline_cpus) != 0) {
    TP_THROW_SYSTEM(ENOTSUP) << "Hot-plug CPU currently not supported.";
  }

  CpuId max_cpu_id_online = 0;
  for (CpuId c = 0; c < CPU_SETSIZE; ++c) {
    if (CPU_ISSET(c, &online_cpus)) {
      max_cpu_id_online = std::max(max_cpu_id_online, c);
    }
  }
  static_assert(kMaxCpus > 0, "!");
  if (max_cpu_id_online > kMaxCpus - 1) {
    TP_THROW_SYSTEM(ENOTSUP)
        << "Larger online  CPU ID found to be " << max_cpu_id_online
        << " but compile-time hard maximum is " << kMaxCpus - 1
        << " Do you want to increase kMaxCpus?";
  }

  if (!cpus.has_value()) {
    // if no value is passed, then use all online CPUs.
    return CpuSet(online_cpus, max_cpu_id_online, max_cpu_id_online);
  }

  // cpus are provided, use them.
  CpuId max_cpu_id = 0;
  for_each_cpu_set_t(c, *cpus) {
    if (CPU_ISSET(c, &(*cpus))) {
      if (!CPU_ISSET(c, &online_cpus)) {
        TP_THROW_SYSTEM(ENOTSUP) << "CPU " << c << " not online";
      }
      max_cpu_id = std::max(max_cpu_id, c);
    }
  }

  return CpuSet(*cpus, max_cpu_id, max_cpu_id_online);
}

CpuSet CpuSet::makeAllOnline() {
  return makeCpuSet(nullopt);
}

CpuSet CpuSet::makeFromCpuSet(cpu_set_t cpu_set) {
  return makeCpuSet(cpu_set);
}

CpuSet CpuSet::getOrDefault(optional<CpuSet> opt) {
  if (opt.has_value()) {
    return *opt;
  } else {
    return makeAllOnline();
  }
}

std::set<CpuId> CpuSet::asSet() const {
  std::set<CpuId> s;
  for_each_cpu(cpu, *this) {
    s.insert(cpu);
  }
  return s;
}

// Return index of next set CPU.
CpuId CpuSet::cpu_set_next(CpuId cpu) const noexcept {
  ++cpu;
  for (; cpu <= max_cpu_id; ++cpu) {
    if (CPU_ISSET(cpu, &cpu_set)) {
      break;
    }
  }
  return cpu;
}

CpuId CpuSet::cpu_first_set() const noexcept {
  if (CPU_ISSET(0, &cpu_set)) {
    return 0u;
  } else {
    return cpu_set_next(0);
  }
}

std::ostream& operator<<(std::ostream& os, const CpuSet& cpu_set) {
  os << "<CpuSet max_cpu_id: " << cpu_set.max_cpu_id
     << " max_cpu_id_online: " << cpu_set.max_cpu_id_online << " cpus: [ ";

  for_each_cpu(cpu, cpu_set) {
    os << cpu << " ";
  }
  os << "] > ";
  return os;
}

// Return index of next set CPU.
CpuId cpu_set_next(CpuId cpu, const cpu_set_t& cpu_set) noexcept {
  ++cpu;
  for (; cpu < kMaxCpus; ++cpu) {
    if (CPU_ISSET(cpu, &cpu_set)) {
      break;
    }
  }
  return cpu;
}

CpuId cpu_first_set(const cpu_set_t& cpu_set) noexcept {
  if (CPU_ISSET(0, &cpu_set)) {
    return 0u;
  } else {
    return cpu_set_next(0, cpu_set);
  }
}

CpuId getCpu() {
  int ret = sched_getcpu();
  if (unlikely(0 > ret)) {
    TP_THROW_SYSTEM(errno) << "Error reading ID of current CPU.";
  }
  if (ret > kMaxCpus - 1) {
    TP_THROW_SYSTEM(ENOTSUP)
        << "CPU ID found to be " << ret << " but compile-time hard maximum is "
        << kMaxCpus - 1;
  }

  return static_cast<CpuId>(ret);
}

} // namespace tensorpipe
