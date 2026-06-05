use std::env;
use std::path::Path;
use std::process::Command;

fn main() {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        print_help();
        return;
    };

    match command.as_str() {
        "help" | "--help" | "-h" => print_help(),
        "sources" => print_sources(),
        "profile" => profile(args.next().as_deref()),
        "fetch" => fetch(args.next().as_deref(), args.next().as_deref()),
        unknown => {
            eprintln!("unknown command: {unknown}");
            print_help();
            std::process::exit(2);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Source {
    id: &'static str,
    url: &'static str,
    note: &'static str,
}

const SOURCES: &[Source] = &[
    Source {
        id: "bis-fx-2025",
        url: "https://www.bis.org/statistics/rpfx25_fx.htm",
        note: "BIS 2025 Triennial Survey summary for global OTC FX turnover and currency shares",
    },
    Source {
        id: "bis-fx-2025-annex",
        url: "https://www.bis.org/statistics/rpfx25_fx_annex.pdf",
        note: "BIS 2025 Triennial Survey annex tables; useful for FX graph weights",
    },
    Source {
        id: "cmc-spot-exchanges",
        url: "https://coinmarketcap.com/rankings/exchanges/",
        note: "CoinMarketCap spot exchange ranking; useful for crypto venue long-tail shape",
    },
    Source {
        id: "marketcap-exchanges",
        url: "https://marketcap.company/stock-exchanges-by-market-cap/",
        note: "Current public stock-exchange market-cap ranking; useful as a rough equity venue count seed",
    },
];

fn print_help() {
    println!(
        "usage:
  cargo run -p exchange_research -- sources
  cargo run -p exchange_research -- profile global-lob
  cargo run -p exchange_research -- fetch <source-id> <output-path>"
    );
}

fn print_sources() {
    for source in SOURCES {
        println!("{} {}", source.id, source.url);
        println!("  {}", source.note);
    }
}

fn profile(name: Option<&str>) {
    match name {
        Some("global-lob") => print_global_lob_profile(),
        Some(profile) => {
            eprintln!("unknown profile: {profile}");
            std::process::exit(2);
        }
        None => {
            eprintln!("profile requires a name");
            std::process::exit(2);
        }
    }
}

fn print_global_lob_profile() {
    let tiers = [
        ("equity-global", 4, 500, 900),
        ("equity-regional", 16, 120, 180),
        ("equity-local", 32, 25, 35),
        ("crypto-major", 12, 350, 260),
        ("crypto-tail", 32, 40, 25),
    ];
    let venues = tiers
        .iter()
        .map(|(_, venue_count, _, _)| venue_count)
        .sum::<usize>();
    let instruments = tiers
        .iter()
        .map(|(_, venue_count, symbols, _)| venue_count * symbols)
        .sum::<usize>();

    println!("profile=global-lob");
    println!("venues={venues}");
    println!("instruments={instruments}");
    for (name, venue_count, symbols, weight) in tiers {
        println!("tier={name} venues={venue_count} instruments_per_venue={symbols} command_weight={weight}");
    }
    println!("notes=shape model only; derive future revisions from research snapshots and scenario configs");
}

fn fetch(source_id: Option<&str>, output_path: Option<&str>) {
    let Some(source_id) = source_id else {
        eprintln!("fetch requires a source id");
        std::process::exit(2);
    };
    let Some(output_path) = output_path else {
        eprintln!("fetch requires an output path");
        std::process::exit(2);
    };
    let Some(source) = SOURCES.iter().find(|source| source.id == source_id) else {
        eprintln!("unknown source: {source_id}");
        std::process::exit(2);
    };

    if let Some(parent) = Path::new(output_path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).expect("create output directory");
        }
    }

    let status = Command::new("curl")
        .args(["-L", "--fail", "--silent", "--show-error", "-o"])
        .arg(output_path)
        .arg(source.url)
        .status()
        .expect("run curl");

    if !status.success() {
        eprintln!("fetch failed for {} from {}", source.id, source.url);
        std::process::exit(status.code().unwrap_or(1));
    }

    println!("ok fetched source={} output={}", source.id, output_path);
}
