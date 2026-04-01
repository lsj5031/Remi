class Remi < Formula
  desc "Unified coding-agent session memory"
  homepage "https://github.com/lsj5031/Remi"
  license "MIT OR Apache-2.0"
  version "0.1.0"

  on_macos do
    odie "macOS release asset is not published yet."
  end

  on_linux do
    if Hardware::CPU.arm?
      odie "Linux arm64 artifact is not published yet"
    else
      url "https://github.com/lsj5031/Remi/releases/download/v0.1.0/remi-linux-x64-simple.tar.gz"
      sha256 "PLACEHOLDER_UPDATE_AFTER_RELEASE"
    end
  end

  def install
    bin.install "remi"
  end

  test do
    assert_match "Unified coding-agent session memory", shell_output("#{bin}/remi --help")
  end
end
