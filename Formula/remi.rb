class Remi < Formula
  desc "Unified coding-agent session memory"
  homepage "https://github.com/lsj5031/Remi"
  license "MIT OR Apache-2.0"
  version "0.2.2"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/lsj5031/Remi/releases/download/v0.2.2/remi-macos-arm64-simple.tar.gz"
      sha256 "ca58bd4034bbc876cf5a6f967db759eaa4e04b1919761405f0e29b4cc6a57796"
    else
      odie "macOS x86_64 release asset is not published yet."
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      odie "Linux arm64 artifact is not published yet"
    else
      url "https://github.com/lsj5031/Remi/releases/download/v0.2.2/remi-linux-x64-simple.tar.gz"
      sha256 "bab6f392f22850620ce3dbcf72554fd7fc604d10745f804d467b64e13d4fa160"
    end
  end

  def install
    bin.install "remi"
  end

  test do
    assert_match "Unified coding-agent session memory", shell_output("#{bin}/remi --help")
  end
end
