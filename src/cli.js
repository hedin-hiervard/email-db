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
import MailComposer from 'nodemailer/lib/mail-composer'

const DB_FILE = 'data/db.json'
const EmailRegex = /(([^<>()\[\]\\.,;:\s@"]+(\.[^<>()\[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))/

type Tag = string;
type Email = string;
type Tags = Set<Tag>;
type Locale = string;
type Status = 'dirty' | 'broken' | 'clean';

type Record = {
    tags: Tags,
    locale?: Locale,
    status: Status,
    lastDelivered?: string,
};

type DB = { [ Email ]: Record };

function filterEmail(line: string): ?Email {
    const match = line.match(EmailRegex)
    if(!match) {
        return null
    }
    return match[0]
}

function readEmailFile(filename: string): Array<Email> {
    log.info(`reading emails from ${filename}`)
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

    log.info(`read ${result.length} emails`)
    return result
}

function saveEmailFile(emails: Array<Email>, filename: string): void {
    log.info(`saving ${emails.length} to ${filename}`)
    fs.writeFileSync(filename, emails.join('\n'))
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

            if(rec.broken) {
                rec.status = 'broken'
            } else if(rec.clean) {
                rec.status = 'clean'
            }
            delete rec.clean
            delete rec.dirty
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
        statusOnly,
        coldDays,
    }: {
        tags: Tags,
        locale?: Locale,
        statusOnly?: Status,
        coldDays?: number,
    }): DB {
        const inputTags = Array.from(tags)

        const result = {}
        for(const email in this.db) {
            const rec = this.db[email]

            if(statusOnly != null && rec.status !== statusOnly) {
                continue
            }
            if((statusOnly == null || statusOnly !== 'broken') &&
                rec.status === 'broken') {
                continue
            }

            if(coldDays && rec.lastDelivered) {
                const d = moment().diff(rec.lastDelivered, 'days')
                if(d < coldDays) {
                    continue
                }
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
                this.db[email] = {
                    tags: new Set(),
                    status: 'dirty',
                }
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

    setStatus(emails: Array<Email>, status: Status): number {
        let res = 0
        for(const email in this.db) {
            const rec = this.db[email]
            if(emails.includes(email)) {
                if(rec.status !== status) {
                    res++
                }
                rec.status = status
            }
        }
        return res
    }

    setLastDelivered(emails: Array<Email>, lastDelivered: *): number {
        let res = 0
        for(const email of emails) {
            const rec = this.db[email]
            if(!rec) {
                continue
            }
            res++
            rec.lastDelivered = lastDelivered.format()
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
        result += `(${rec.status})`
        if(rec.lastDelivered) {
            result += `[lastSent:${rec.lastDelivered}]`
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
    .option('--status <status>')
    .option('--cold-days <coldDays>')
    .description('queries email with tags')
    .action(async ({ tag: tags, locale, status, coldDays }) => {
        log.info(`emails with ALL of the tags: ${tags.join(', ')}`)
        if(locale) {
            log.info(`locale: ${locale}`)
        }
        if(status && !['clean', 'dirty', 'broken'].includes(status)) {
            log.error('invalid status')
            process.exit(1)
        }

        const result = db.lookup({
            tags: new Set(tags || []),
            locale,
            statusOnly: status,
            coldDays,
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
    const ts = moment().format('YYYY-MM-DD-HH.MM')

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
        tokens.push(`${inputId}=${str.toString()}`)
    }
    filename += `${tokens.join(',')}`

    filename += `.txt`
    return filename
}

program
    .command('export')
    .option('--tag [tag]', 'tags', (val, memo) => { memo.push(val); return memo }, [])
    .option('--locale [locale]', 'locale')
    .option('--status <status>')
    .option('--cold-days <coldDays>')
    .description('queries email with tags')
    .action(async ({ tag: tags, locale, status, coldDays }) => {
        log.info(`emails with ALL of the tags: ${tags.join(', ')}`)
        if(locale) {
            log.info(`locale: ${locale}`)
        }
        if(status && !['clean', 'dirty', 'broken'].includes(status)) {
            log.error('invalid status')
            process.exit(1)
        }
        const result = db.lookup({
            tags: new Set(tags || []),
            locale,
            statusOnly: status,
            coldDays,
        })
        const emails = Object.keys(result)
        const filename = makeFilename(
            'export',
            {
                tags,
                locale,
                statusOnly: status,
                coldDays,
            }
        )
        saveEmailFile(emails, filename)
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
        })
        let emails = []
        for(const supressType of [ 'unsubscribes', 'bounces', 'complaints' ]) {
            log.info(`getting ${supressType}`)
            const res = await pageAll(mg, mg[supressType]().list.bind(mg[supressType]()), 100)
            log.info(`got ${res.length} ${supressType}`)
            emails = [ ...emails, ...res.map(item => item.address) ]
        }
        const filename = makeFilename('downloaded', {
            broken: true,
        })
        saveEmailFile(emails, filename)
    })

program
    .command('set_delivered_now <filename>')
    .action(async (filename) => {
        const emails = readEmailFile(filename)
        const res = db.setLastDelivered(emails, moment())
        log.info(`${res} emails updated`)
        db.save()
    })

program
    .command('update_delivered')
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
        saveEmailFile(emails, filename)

        let res = 0
        for(const item of response) {
            const ts = moment.utc(item.timestamp * 1000)
            res += db.setLastDelivered([ item.recipient ], ts)
        }
        log.info(`${res} emails updated`)
        db.save()
    })

program
    .command('mark_clean <filename>')
    .action(async (filename) => {
        const emails = readEmailFile(filename)
        log.info(`marking clean`)
        const res = db.setStatus(emails, 'clean')
        log.info(`${res} changed status`)
        db.save()
    })

program
    .command('mark_broken <filename>')
    .action(async (filename) => {
        const emails = readEmailFile(filename)
        log.info(`marking BROKEN`)
        const res = db.setStatus(emails, 'broken')
        log.info(`${res} changed status`)
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

program
    .command('send <listName> <subject> <htmlFile>')
    .action(async (listName, subject, htmlFile) => {
        const html = fs.readFileSync(htmlFile)

        if(process.env.MAILGUN_DOMAIN == null) {
            log.error(`set MAILGUN_DOMAIN env var`)
            return
        }

        const to = `${listName}@${process.env.MAILGUN_DOMAIN}`
        const mailOptions = {
            from: process.env.FROM_ADDRESS,
            to,
            subject,
            text: '',
            html,
        }
        var mail = new MailComposer(mailOptions)

        const message = await mail.compile().build()

        const dataToSend = {
            to,
            message: message.toString('ascii'),
        }

        const mg = Mailgun({
            apiKey: process.env.MAILGUN_API_KEY,
            domain: process.env.MAILGUN_DOMAIN,
        })

        const result = await mg.messages().sendMime(dataToSend)
        log.info(result)
    })

program.on('command:*', function () {
    program.help()
})

program.parse(process.argv)

if (!process.argv.slice(2).length) {
    program.outputHelp()
}
