#!/usr/bin/env python3
"""
Verify and fix classification errors in Reddit data CSV using link_flair_text.

Usage:
    python3 fix_csv.py                    # Verify only (dry run)
    python3 fix_csv.py --fix              # Verify and apply fixes
    python3 fix_csv.py --stats            # Show flair statistics
"""

import csv
import argparse
from typing import Optional

# =============================================================================
# CONFIGURATION
# =============================================================================

CSV_FILE = 'reddit_crunchyroll_5days.csv'

# Flair → Expected issue_type(s) mapping
FLAIR_TO_ISSUE_TYPE = {
    'News': ['Announcements'],
    'Megathread': ['Announcements'],
    'Streaming | Technical Issue': ['Playback', 'UX/App', 'Account'],
    'Account/Membership': ['Account', 'Billing'],
    'Streaming Catalog': ['Licensing/Catalog'],
    'Service Discussion': ['General Discussion', 'UX/App'],
    'Dubs': ['Content', 'Licensing/Catalog', 'Playback', 'Announcements'],
    'Simulcast': ['Licensing/Catalog', 'Announcements', 'Playback'],
    'Prime Time | YouTube': ['Licensing/Catalog', 'General Discussion'],
    'Prime Time | Amazon Prime': ['Licensing/Catalog', 'General Discussion', 'Account'],
    '3rd Party Article': ['Announcements', 'General Discussion'],
}

# Flairs where device should be Unknown
FLAIRS_NO_DEVICE = {
    'News', 'Megathread', 'Streaming Catalog', 'Service Discussion',
    'Simulcast', 'Prime Time | YouTube', 'Prime Time | Amazon Prime', '3rd Party Article',
}

# Manual fixes: post_id -> {field: new_value}
MANUAL_FIXES = {}

# =============================================================================
# VALIDATION RULES
# =============================================================================

def check_flair_issue_type(row: dict) -> Optional[dict]:
    """Validate issue_type based on link_flair_text."""
    flair = row.get('link_flair_text', '')
    issue_type = row.get('issue_type', '')
    if flair in FLAIR_TO_ISSUE_TYPE:
        expected_types = FLAIR_TO_ISSUE_TYPE[flair]
        if issue_type not in expected_types:
            return {'issue_type': expected_types[0]}
    return None


def check_flair_device(row: dict) -> Optional[dict]:
    """Validate that announcement/catalog flairs have Unknown device."""
    flair = row.get('link_flair_text', '')
    device = row.get('device', '')
    if flair in FLAIRS_NO_DEVICE and device != 'Unknown':
        return {'device': 'Unknown'}
    return None


def check_manual_fixes(row: dict) -> Optional[dict]:
    """Apply manual fixes for specific posts."""
    post_id = row.get('id', '')
    if post_id in MANUAL_FIXES:
        fixes = {}
        for field, expected in MANUAL_FIXES[post_id].items():
            if row.get(field) != expected:
                fixes[field] = expected
        return fixes if fixes else None
    return None


VALIDATORS = [
    ('Flair → Issue Type', check_flair_issue_type),
    ('Flair → No Device', check_flair_device),
    ('Manual Fixes', check_manual_fixes),
]

# =============================================================================
# MAIN LOGIC
# =============================================================================

def load_csv(filepath: str) -> tuple:
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return reader.fieldnames, list(reader)


def save_csv(filepath: str, fieldnames: list, rows: list) -> None:
    with open(filepath, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def verify_and_fix(apply_fixes: bool = False) -> None:
    fieldnames, rows = load_csv(CSV_FILE)
    issues_found, fixes_to_apply = [], {}
    
    for row in rows:
        post_id, title = row['id'], row['title'][:50]
        flair = row.get('link_flair_text', 'N/A')
        
        for rule_name, validator in VALIDATORS:
            fixes = validator(row)
            if fixes:
                if post_id not in fixes_to_apply:
                    fixes_to_apply[post_id] = {}
                for field, new_value in fixes.items():
                    old_value = row[field]
                    if field not in fixes_to_apply[post_id]:
                        fixes_to_apply[post_id][field] = (old_value, new_value)
                        issues_found.append({
                            'post_id': post_id, 'title': title, 'flair': flair,
                            'rule': rule_name, 'field': field, 'old': old_value, 'new': new_value,
                        })
    
    print(f"\n{'='*80}\nVERIFICATION REPORT - {CSV_FILE}\n{'='*80}")
    print(f"Total rows: {len(rows)} | Issues: {len(issues_found)} | Posts to fix: {len(fixes_to_apply)}")
    
    if issues_found:
        print(f"\n{'-'*80}\nISSUES DETECTED:\n{'-'*80}")
        current_post = None
        for issue in issues_found:
            if issue['post_id'] != current_post:
                current_post = issue['post_id']
                print(f"\n📌 {issue['post_id']} [{issue['flair']}]: {issue['title']}...")
            print(f"   [{issue['rule']}] {issue['field']}: '{issue['old']}' → '{issue['new']}'")
    
    if apply_fixes and fixes_to_apply:
        print(f"\n{'-'*80}\nAPPLYING FIXES...\n{'-'*80}")
        for row in rows:
            if row['id'] in fixes_to_apply:
                for field, (old, new) in fixes_to_apply[row['id']].items():
                    row[field] = new
                    print(f"✅ {row['id']}: {field} '{old}' → '{new}'")
        save_csv(CSV_FILE, fieldnames, rows)
        print(f"\n💾 Saved {len(fixes_to_apply)} fixes to {CSV_FILE}")
    elif fixes_to_apply:
        print(f"\n⚠️  Run with --fix to apply these changes")
    else:
        print(f"\n✅ All classifications correct!")


def show_flair_stats() -> None:
    """Show distribution of flairs and their current classifications."""
    _, rows = load_csv(CSV_FILE)
    flair_stats = {}
    for row in rows:
        flair = row.get('link_flair_text', 'N/A') or 'N/A'
        issue, device = row.get('issue_type', 'Unknown'), row.get('device', 'Unknown')
        if flair not in flair_stats:
            flair_stats[flair] = {'count': 0, 'issues': {}, 'devices': {}}
        flair_stats[flair]['count'] += 1
        flair_stats[flair]['issues'][issue] = flair_stats[flair]['issues'].get(issue, 0) + 1
        flair_stats[flair]['devices'][device] = flair_stats[flair]['devices'].get(device, 0) + 1
    
    print(f"\n{'='*80}\nFLAIR STATISTICS\n{'='*80}")
    for flair, stats in sorted(flair_stats.items(), key=lambda x: -x[1]['count']):
        print(f"\n📁 {flair} ({stats['count']} posts)")
        print(f"   Issue Types: {dict(sorted(stats['issues'].items(), key=lambda x: -x[1]))}")
        print(f"   Devices: {dict(sorted(stats['devices'].items(), key=lambda x: -x[1]))}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Verify and fix CSV classifications using flair.')
    parser.add_argument('--fix', action='store_true', help='Apply fixes')
    parser.add_argument('--stats', action='store_true', help='Show flair statistics')
    args = parser.parse_args()
    
    if args.stats:
        show_flair_stats()
    else:
        verify_and_fix(apply_fixes=args.fix)
