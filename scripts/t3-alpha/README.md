# t3-alpha

Zero-dependency Node (>= 22) console client for a T3 Code server (wire format of v0.0.45).

    node scripts/t3-alpha/t3-alpha.mjs login --url http://127.0.0.1:5773 --pair <code from `t3 pair`>
    node scripts/t3-alpha/t3-alpha.mjs create --project <id> --title "Zgłoszenie 588" --prompt "..."
    node scripts/t3-alpha/t3-alpha.mjs read --thread <id>

Config: ~/.config/t3-alpha.json ({ baseUrl, token }). Model, runtime and interaction mode come from the server (project default or latest thread in the project).
