// @flow
import Youch from 'youch'
import forTerminal from 'youch-terminal'
import program from 'commander'
import { StreamLogger } from 'ual'
import fs from 'fs-extra'
import _ from 'lodash'

const DB_FILE = 'data/db.json'
const EmailRegex = /(([^<>()\[\]\\.,;:\s@"]+(\.[^<>()\[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))/

type Tag = string;
type Email = string;
type Tags = Set<Tag>;
type Locale = string;

type Record = {
    tags: Tags,
    locale?: Locale,
    broken?: boolean,
};

type DB = { [ Email ]: Record }

class EmailDB {
    db: DB;

    filter(line: string): ?Email {
        const match = line.match(EmailRegex)
        if(!match) {
            return null
        }
        return match[0]
    }

    guessEmailLocale(email: Email): Locale {
        if(email.match(/\.ru$/)) {
            return 'ru'
        }
        return 'en'
    }

    load() {
        try {
            this.db = JSON.parse(fs.readFileSync(DB_FILE, 'utf-8'))
        } catch(err) {
            this.db = {}
        }
    }

    save() {
        if(fs.existsSync(DB_FILE)) {
            fs.renameSync(DB_FILE, `${DB_FILE}.tmp`)
        }
        fs.writeFileSync(DB_FILE, JSON.stringify(this.db, null, 4))
    }

    lookup({
        tags,
        locale,
    }: {
        tags: Tags,
        locale?: Locale,
    }): DB {
        const inputTags = Array.from(tags)

        const result = {}
        for(const email in this.db) {
            const rec = this.db[email]
            const recTags = Array.from(rec.tags)
            if(_.intersection(recTags, inputTags).length < tags.size) {
                continue
            }
            const calculatedLocale = rec.locale || this.guessEmailLocale(email)
            if(locale != null && calculatedLocale !== locale) {
                continue
            }
            result[email] = {
                tags: new Set(recTags),
                locale: calculatedLocale,
            }
        }
        return result
    }

    allTags(): {
        [ Tag ]: number,
        } {
        let result = {}
        for(const email in this.db) {
            const rec = this.db[email]
            for(const tag of rec.tags) {
                if(!result[tag]) { result[tag] = 0 }
                result[tag]++
            }
        }
        return result
    }

    tags(email: Email): Tags {
        if(this.db[email] == null) {
            return new Set()
        }
        return this.db[email].tags
    }

    insert(filename: string, tags: Tags): {
        inserted: number,
        updated: number,
    } {
        let result = {
            inserted: 0,
            updated: 0,
        }
        const lines = fs.readFileSync(filename, 'utf-8').split('\n')
        for(let line of lines) {
            const email = this.filter(line)
            if(!email) {
                continue
            }
            if(this.db[email] == null) {
                this.db[email] = { tags: new Set() }
                result.inserted++
            } else {
                result.updated++
            }
            const res = this.db[email]
            res.tags = new Set([ ...res.tags, ...tags ])
        }
        return result
    }

    cleanup(): {
        deleted: number,
        fixed: number,
        } {
        const result = {
            deleted: 0,
            fixed: 0,
        }
        for(const email in this.db) {
            const rec = this.db[email]
            const filtered = this.filter(email)
            if(!filtered) {
                delete this.db[email]
                result.deleted++
            } else if(filtered !== email) {
                result.fixed++
                this.db[filtered] = rec
                delete this.db[email]
            }
        }
        return result
    }

    totalEmails(): number {
        return Object.keys(this.db).length
    }
}

const log = new StreamLogger({ stream: process.stdout })
const db = new EmailDB()
db.load()

process.on('unhandledRejection', err => {
    throw err
})

process.on('uncaughtException', err => {
    new Youch(err, {})
        .toJSON()
        .then((output) => {
            log.error(forTerminal(output))
        })
})

program
    .command('insert <filename>')
    .option('--tag [tag]', 'tags', (val, memo) => { memo.push(val); return memo }, [])
    .description('inserts all emails with one or more tags')
    .action(async (filename, { tag: tags }) => {
        const result = db.insert(filename, new Set(tags))
        log.info(`inserted ${result.inserted} new emails, updated tags on ${result.updated} emails`)
        db.save()
    })

program
    .command('query')
    .option('--tag [tag]', 'tags', (val, memo) => { memo.push(val); return memo }, [])
    .option('--locale [locale]', 'locale')
    .description('queries email with tags')
    .action(async ({ tag: tags, locale }) => {
        log.info(`emails with ALL of the tags: ${tags.join(', ')}`)
        if(locale) {
            log.info(`locale: ${locale}`)
        }
        const result = db.lookup({
            tags: new Set(tags || []),
            locale,
        })
        for(const email in result) {
            const rec = result[email]
            log.info(`${email}: (${Array.from(rec.tags).join(',')}), ${rec.locale || '?'}`)
        }
        log.info(`${Object.keys(result).length} total`)
        db.save()
    })

program
    .command('tags')
    .description('shows all tags with stats')
    .action(async () => {
        const tags = db.allTags()
        for(const tag in tags) {
            log.info(`${tag}: ${tags[tag]} emails`)
        }
    })

program
    .command('cleanup')
    .description('cleans the db of bad emails')
    .action(async () => {
        const { fixed, deleted } = db.cleanup()
        log.debug(`${fixed} fixed, ${deleted} purged`)
        db.save()
    })

program.on('command:*', function () {
    program.help()
})

program.parse(process.argv)

if (!process.argv.slice(2).length) {
    program.outputHelp()
}
