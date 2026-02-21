class Remi < Formula
  desc "Unified coding-agent session memory"
  homepage "https://github.com/lsj5031/Remi"
  license "MIT OR Apache-2.0"
  version "0.0.4"

  on_macos do
    odie "macOS release asset is not published yet."
  end

  on_linux do
    if Hardware::CPU.arm?
      odie "Linux arm64 artifact is not published yet"
    else
      url "https://github.com/lsj5031/Remi/releases/download/v0.0.4/remi-linux-x64-simple.tar.gz"
      sha256 "8236aafd908a3ca346d9421a8d39b14ae7d366c8fb5c98579622146ef13fca85"
    end
  end

  def install
    bin.install "remi"
  end

  test do
    assert_match "Unified coding-agent session memory", shell_output("#{bin}/remi --help")
  end
end
