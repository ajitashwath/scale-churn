import logging
import os
import sys
from typing import Optional

logger = logging.getLogger(__name__)

def setup_java_home() -> None:
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        java_exe = os.path.join(java_home, "bin", "java.exe" if os.name == 'nt' else "java")
        if os.path.exists(java_exe):
            logger.info("JAVA_HOME already set to: %s", java_home)
            return
    
    search_paths = []
    if os.name == 'nt':
        # Windows search paths
        local_appdata = os.environ.get("LOCALAPPDATA", "")
        if local_appdata:
            search_paths.append(os.path.join(local_appdata, "Java", "jdk-17"))
        
        program_files = os.environ.get("ProgramFiles", "")
        program_files_x86 = os.environ.get("ProgramFiles(x86)", "")
        
        if program_files:
            search_paths.extend([
                os.path.join(program_files, "Java"),
                os.path.join(program_files, "Eclipse Adoptium"),
            ])
        if program_files_x86:
            search_paths.append(os.path.join(program_files_x86, "Java"))
        
        if local_appdata:
            search_paths.append(os.path.join(local_appdata, "Programs", "Java"))
    else:
        search_paths.extend([
            "/usr/lib/jvm/default-java",
            "/usr/lib/jvm/java-11-openjdk-amd64",
            "/usr/lib/jvm/java-17-openjdk-amd64",
            "/Library/Java/JavaVirtualMachines",
        ])
    
    for base_path in search_paths:
        if not os.path.exists(base_path):
            continue
        
        java_exe = os.path.join(base_path, "bin", "java.exe" if os.name == 'nt' else "java")
        if os.path.exists(java_exe):
            os.environ["JAVA_HOME"] = base_path
            logger.info("Found Java and set JAVA_HOME to: %s", base_path)
            return
        try:
            for item in os.listdir(base_path):
                jdk_path = os.path.join(base_path, item)
                if os.path.isdir(jdk_path):
                    java_exe = os.path.join(jdk_path, "bin", "java.exe" if os.name == 'nt' else "java")
                    if os.path.exists(java_exe):
                        os.environ["JAVA_HOME"] = jdk_path
                        logger.info("Found Java and set JAVA_HOME to: %s", jdk_path)
                        return
        except (OSError, PermissionError):
            continue
    
    logger.error("Java not found! Please install Java and set JAVA_HOME.")
    if os.name == 'nt':
        logger.error("You can run: .\\scripts\\setup_java.ps1")
    raise RuntimeError(
        "JAVA_HOME is not set and Java could not be found automatically. "
        "Please install Java (JDK 8 or later) and set JAVA_HOME"
    )


def setup_hadoop_windows() -> None:
    if os.name != 'nt':
        return
    
    if not os.environ.get("HADOOP_HOME") and not os.environ.get("HADOOP_HOME_DIR"):
        dummy_hadoop_home = os.path.join(os.path.expanduser("~"), ".hadoop")
        bin_dir = os.path.join(dummy_hadoop_home, "bin")
        os.makedirs(bin_dir, exist_ok=True)
        
        winutils_path = os.path.join(bin_dir, "winutils.exe")
        if not os.path.exists(winutils_path):
            try:
                import urllib.request
                winutils_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.0/bin/winutils.exe"
                logger.info("Downloading winutils.exe for Windows Hadoop support...")
                urllib.request.urlretrieve(winutils_url, winutils_path)
                logger.info("winutils.exe downloaded successfully")
            except Exception as e:
                logger.warning(f"Could not download winutils.exe: {e}. Spark may have issues on Windows.")
        
        os.environ["HADOOP_HOME"] = dummy_hadoop_home
        logger.info("Set HADOOP_HOME to: %s", dummy_hadoop_home)


def configure_logging(name: str, level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return logging.getLogger(name)


def validate_data_path(path: str, must_exist: bool = True) -> str:
    abs_path = os.path.abspath(path)
    if must_exist and not os.path.exists(abs_path):
        raise FileNotFoundError(f"Data path does not exist: {abs_path}")
    return abs_path
