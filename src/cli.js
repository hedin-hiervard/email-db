// @flow
import Youch from 'youch'
import forTerminal from 'youch-terminal'
import program from 'commander'
import { StreamLogger } from 'ual'
import fs from 'fs-extra'
import _ from 'lodash'
import moment from 'moment'
import Mailgun from 'mailgun-js'
import dotenv from 'dotenv'

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
    clean?: boolean,
};

type DB = { [ Email ]: Record };

function filterEmail(line: string): ?Email {
        const match = line.match(EmailRegex)
        if(!match) {
            return null
        }
        return match[0]
    }

class EmailDB {
    db: DB;



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
        for(const email in this.db) {
            const rec = this.db[email]
            rec.tags = new Set(rec.tags)
        }
    }

    save() {
        if(fs.existsSync(DB_FILE)) {
            fs.renameSync(DB_FILE, `${DB_FILE}.tmp`)
        }
        fs.writeFileSync(DB_FILE, JSON.stringify(this.db, null, 4))
    }

    lookupByRegexp(regexp: RegExp): DB {
        const res = {}
        for(const email in this.db) {
            if(email.match(regexp)) {
                res[email] = this.db[email]
            }
        }
        return res
    }

    lookup({
        tags,
        locale,
        cleanOnly,
        dirtyOnly,
    }: {
        tags: Tags,
        locale?: Locale,
        cleanOnly?: boolean,
        dirtyOnly?: boolean,
    }): DB {
        const inputTags = Array.from(tags)

        const result = {}
        for(const email in this.db) {
            const rec = this.db[email]
            if(rec.broken) {
                continue
            }
            if(!rec.clean && cleanOnly) {
                continue
            }
            if(rec.clean && dirtyOnly) {
                continue
            }
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
            for(const tag of Array.from(rec.tags)) {
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

    insert({
        filename,
        tags,
        locale,
    }: {
        filename: string,
        tags: Tags,
        locale?: Locale,
    }): {
        inserted: number,
        updated: number,
    } {
        let result = {
            inserted: 0,
            updated: 0,
        }
        const lines = fs.readFileSync(filename, 'utf-8').split('\n')
        for(let line of lines) {
            const email = filterEmail(line)
            if(!email) {
                continue
            }
            if(this.db[email] == null) {
                this.db[email] = { tags: new Set() }
                result.inserted++
            }
            const res = this.db[email]
            let addedTags = false
            for(const tag of Array.from(tags)) {
                if(!res.tags.has(tag)) {
                    res.tags.add(tag)
                    addedTags = true
                }
            }
            res.tags = new Set([ ...res.tags, ...tags ])
            if(addedTags || (locale && locale !== res.locale)) {
                result.updated++
            }
            if(locale) {
                res.locale = locale
            }
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
            const filtered = filterEmail(email)
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

    markAsBroken(emails: Array<Email>): number {
        let res = 0
        for(const email in this.db) {
            const rec = this.db[email]
            if(emails.includes(email)) {
                if(!rec.broken) {
                    res++
                }
                rec.broken = true
            }
        }
        return res
    }
    markAsClean(emails: Array<Email>): number {
        let res = 0
        for(const email in this.db) {
            const rec = this.db[email]
            if(!rec.clean && emails.includes(email)) {
                res++
                rec.clean = true
                delete rec.broken
            }
        }
        return res
    }

    emailToString(email: Email): string {
        const rec = this.db[email]
        if(!rec) {
            return `${email}: not found in db`
        }
        const locale = rec.locale || this.guessEmailLocale(email) || '?'
        let result = `${email}:`
        if(rec.tags.size > 0) {
            result += `(${Array.from(rec.tags).join(',')})`
        }
        result += `[${locale}]`
        if(rec.clean) {
            result += `(clean!)`
        } else {
            result += `(dirty)`
        }
        if(rec.broken) {
            result += `[ !! BROKEN !! ]`
        }
        return result
    }
}

dotenv.config()

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
    .option('--locale [locale]', 'locale')
    .description('inserts all emails with one or more tags')
    .action(async (filename, { tag: tags, locale }) => {
        const result = db.insert({
            filename,
            tags: new Set(tags),
            locale,
        })
        log.info(`inserted ${result.inserted} new emails, updated ${result.updated} emails`)
        db.save()
    })

program
    .command('query')
    .option('--tag [tag]', 'tags', (val, memo) => { memo.push(val); return memo }, [])
    .option('--locale [locale]', 'locale')
    .option('--clean-only')
    .option('--dirty-only')
    .description('queries email with tags')
    .action(async ({ tag: tags, locale, cleanOnly, dirtyOnly }) => {
        log.info(`emails with ALL of the tags: ${tags.join(', ')}`)
        if(locale) {
            log.info(`locale: ${locale}`)
        }
        const result = db.lookup({
            tags: new Set(tags || []),
            locale,
            cleanOnly,
            dirtyOnly,
        })
        for(const email in result) {
            log.info(db.emailToString(email))
        }
        log.info(`${Object.keys(result).length} total`)
    })

function makeFilename(
    base: string,
    input: {
        [ string ]: *,
    }
): string {
    const ts = moment().format('YYYY-MM-DD:HH.MM')

    let filename = `${base}/`
    const tokens = [ ts ]
    for(const inputId in input) {
        let str
        if(input[inputId] == null) {
            continue
        }
        if(Array.isArray(input[inputId])) {
            str = input[inputId].join(',')
        } else {
            str = input[inputId]
        }
        tokens.push(`${inputId}:${str.toString()}`)
    }
    filename += `${tokens.join('-')}`

    filename += `.txt`
    return filename
}

function readEmailFile(filename: string): Array<Email> {
    const result = []
    let lines
    try {
        lines = fs.readFileSync(filename, 'utf-8').split('\n')
    } catch(err) {
        return []
    }

    for(const line of lines) {
        let email = filterEmail(line)
        if(email) {
            result.push(email)
        }
    }

    return result
}

program
    .command('export')
    .option('--tag [tag]', 'tags', (val, memo) => { memo.push(val); return memo }, [])
    .option('--locale [locale]', 'locale')
    .option('--clean-only')
    .option('--dirty-only')
    .description('queries email with tags')
    .action(async ({ tag: tags, locale, cleanOnly, dirtyOnly }) => {
        log.info(`emails with ALL of the tags: ${tags.join(', ')}`)
        if(locale) {
            log.info(`locale: ${locale}`)
        }
        const result = db.lookup({
            tags: new Set(tags || []),
            locale,
        })
        const emails = Object.keys(result)
        const filename = makeFilename(
            'export',
            {
                tags,
                locale,
                cleanOnly,
                dirtyOnly,
            }
        )
        log.info(`saving ${emails.length} emails to ${filename}`)
        fs.writeFileSync(filename, emails.join('\n'))
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
        log.info(`${fixed} fixed, ${deleted} purged`)
        db.save()
    })

async function pageAll(
    mg,
    fn,
    limit: number,
): Promise<Array<*>> {
    let result = []
    let url

    while(true) {
        let response
        if(url) {
            response = await mg.get(url)
        } else {
            response = await fn({
                limit,
            })
        }
        if(response.items.length === 0) {
            break
        }
        result = [ ...result, ...response.items ]
        url = response.paging.next.split('https://api.mailgun.net/v3')[1]
    }
    return result
}

program
    .command('download_broken')
    .description('asks mailgun for failed deliveries, complains and unsubscribes')
    .action(async () => {
        const mg = Mailgun({
            apiKey: process.env.MAILGUN_API_KEY,
            domain: process.env.MAILGUN_DOMAIN,
            // testMode: true,
        })
        let emails = []
        for(const supressType of [ 'unsubscribes', 'bounces', 'complaints' ]) {
            log.info(`getting ${supressType}`)
            const res = await pageAll(mg, mg[supressType]().list.bind(mg[supressType]()), 100)
            log.info(`got ${res.length} ${supressType}`)
            emails = [ ...emails, ...res.map(item => item.address) ]
        }
        const result = db.markAsBroken(emails)
        log.info(`${result} emails marked as broken`)
        db.save()
    })

program
    .command('download_delivered')
    .action(async () => {
        const mg = Mailgun({
            apiKey: process.env.MAILGUN_API_KEY,
            domain: process.env.MAILGUN_DOMAIN,
        })
        const response = await pageAll(mg, mg.events().get.bind(mg.events(), {
            event: 'delivered',
        }), 100)
        const emails = _.uniq(response.map(item => item.recipient))
        log.info(`${emails.length} delivered emails`)
        const filename = makeFilename('downloaded', {
            delivered: true,
        })
        log.info(`saving to ${filename}`)
        fs.writeFileSync(filename, emails.join('\n'))
    })

program
    .command('mark_clean <filename>')
    .action(async (filename) => {
        log.info(`reading from ${filename}`)
        const emails = readEmailFile(filename)
        log.info(`read ${emails.length} emails, marking clean`)
        db.markAsClean(emails)
        db.save()
    })

program
    .command('mark_broken <filename>')
    .action(async (filename) => {
        log.info(`reading from ${filename}`)
        const emails = readEmailFile(filename)
        log.info(`read ${emails.length} emails, marking BROKEN`)
        db.markAsBroken(emails)
        db.save()
    })

program
    .command('find_emails <regexp>')
    .action(async (regexp) => {
        const result = db.lookupByRegexp(new RegExp(regexp))
        log.info(`found ${Object.keys(result).length}`)
        for(const email in result) {
            log.info(db.emailToString(email))
        }
    })

program.on('command:*', function () {
    program.help()
})

program.parse(process.argv)

if (!process.argv.slice(2).length) {
    program.outputHelp()
}
