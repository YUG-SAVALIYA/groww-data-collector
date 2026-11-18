"""OS-Aware Desktop Detection and Storage Path Resolver.

Detects Desktop across Windows (including OneDrive redirection), Linux (XDG user-dirs),
and macOS, defaulting to 'NSE Equity Companies' while allowing full user overrides.
"""

from __future__ import annotations
import os
import sys
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

DEFAULT_FOLDER_NAME = "NSE Equity Companies"

def get_desktop_directory() -> Path:
    """Detect the active Desktop directory in an OS-aware manner.
    
    Handles Windows OneDrive redirection, Linux XDG desktop dirs, and macOS.
    """
    if sys.platform == "win32":
        # 1. Check Windows Registry User Shell Folders (handles OneDrive Desktop redirection)
        try:
            import winreg
            key = winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                r"Software\Microsoft\Windows\CurrentVersion\Explorer\User Shell Folders"
            )
            val, _ = winreg.QueryValueEx(key, "Desktop")
            winreg.CloseKey(key)
            expanded = os.path.expandvars(val)
            path = Path(expanded)
            if path.exists():
                return path
        except Exception as e:
            logger.debug(f"Windows registry desktop query exception: {e}")

        # 2. Check standard OneDrive path
        onedrive_env = os.environ.get("OneDrive") or os.environ.get("OneDriveConsumer") or os.environ.get("OneDriveCommercial")
        if onedrive_env:
            od_desktop = Path(onedrive_env) / "Desktop"
            if od_desktop.exists():
                return od_desktop

        # 3. Check standard USERPROFILE\Desktop
        home = Path.home()
        user_desktop = home / "Desktop"
        if user_desktop.exists():
            return user_desktop

    elif sys.platform.startswith("linux"):
        # Check XDG user dirs config
        xdg_config = Path.home() / ".config" / "user-dirs.dirs"
        if xdg_config.exists():
            try:
                with open(xdg_config, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.startswith("XDG_DESKTOP_DIR="):
                            raw = line.split("=", 1)[1].strip().strip('"')
                            raw = raw.replace("$HOME", str(Path.home()))
                            p = Path(raw)
                            if p.exists():
                                return p
            except Exception:
                pass

        # Standard Linux Desktop
        linux_desktop = Path.home() / "Desktop"
        if linux_desktop.exists():
            return linux_desktop

    else:
        # macOS or others
        mac_desktop = Path.home() / "Desktop"
        if mac_desktop.exists():
            return mac_desktop

    # Fallback: Home directory if no Desktop exists
    return Path.home()

def resolve_storage_directory(
    custom_dir: Path | str | None = None,
    folder_name: str = DEFAULT_FOLDER_NAME,
    ensure_created: bool = True,
) -> tuple[Path, bool, Path]:
    """Resolve target storage directory.
    
    Returns:
        (target_data_dir, already_existed, detected_desktop_path)
    """
    desktop = get_desktop_directory()

    if custom_dir:
        target_path = Path(custom_dir).resolve()
    else:
        target_path = (desktop / folder_name).resolve()

    already_existed = target_path.exists()

    if ensure_created:
        target_path.mkdir(parents=True, exist_ok=True)

    return target_path, already_existed, desktop
