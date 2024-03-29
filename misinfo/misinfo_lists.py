blacklist = ['cdc.gov',
 'covid.cdc.gov',
 'nhs.uk',
 'fda.gov',
 'hhs.gov',
 'who.int',
 'kff.org.',
 'immunology.org',
 'pewtrusts.org',
 'nature.com',
 'chop.edu',
 'alzheimers.org.uk',
 'sciencefeedback.co',
 'facebook.com/sharer',
 'coronavirus.jhu.edu',
 'tz.usembassy.gov',
 'cidrap.umn.edu',
 'thehill.com',
 'nytimes.com',
 'reuters.com',
 'gov.uk',
 'coronavirus.data.gov.uk',
 'worldometers.info',
 'lpi.oregonstate.edu'
]

archive_domains = ['perma.cc',
 'archive.is',
 'archive.md',
 'archive.vn',
 'archive.ph',
 'web.archive.org',
 'archive.fo'
]

known_misinfo = ['thelightpaper.co.uk']

whitelist_priority = known_misinfo + archive_domains + ['facebook.com', 'twitter.com']

search_terms = [
            'ivermectin',
            'cancer',
            'miscarriage',
            'asymptomatic',
            'chloroquine',
            'mask',
            'mandate',
            'certificate',
            'travel',
            'lockdown',
            'unvaccinated',
            'booster',
            'neurodegenerative',
            'aspirin',
            'autopsies',
            'hypoxia',
            'arrested',
            'impotence',
            'hospitalized',
            'parasite',
            'azithromycin'
        ]
        
archive_links_that_are_not_misinfo = [
    'https://web.archive.org/web/20220111040020/https://www.gov.uk/order-coronavirus-rapid-lateral-flow-tests',
    'https://web.archive.org/web/20210616204557/https://www.gov.uk/government/publications/coronavirus-covid-19-vaccine-adverse-reactions/coronavirus-vaccine-summary-of-yellow-card-reporting',
    'https://web.archive.org/web/20210721024320/https://www.gov.uk/government/publications/coronavirus-covid-19-vaccine-adverse-reactions/coronavirus-vaccine-summary-of-yellow-card-reporting',
    'https://web.archive.org/web/20210605171951/https://www.gov.uk/government/publications/coronavirus-covid-19-vaccine-adverse-reactions/coronavirus-vaccine-summary-of-yellow-card-reporting',
    'https://web.archive.org/web/20210622075607/https://www.who.int/emergencies/diseases/novel-coronavirus-2019/covid-19-vaccines/advice',
    'https://web.archive.org/web/20210331210646/https://www.cdc.gov/coronavirus/2019-ncov/vaccines/safety/adverse-events.html',
    'https://web.archive.org/web/20200910123516/https://www.cdc.gov/coronavirus/2019-ncov/hcp/planning-scenarios.html',
    'https://web.archive.org/web/20200531064451/https://www.cdc.gov/coronavirus/2019-ncov/hcp/planning-scenarios.html',
    'https://web.archive.org/web/20211109225720/https://www.bmj.com/content/375/bmj.n2635',
    'https://web.archive.org/web/20210726200752/https://covid19.who.int/',
    'https://archive.ph/hLlo7',
    'https://archive.ph/4oKus',
    'http://archive.vn/hLlo7',
    'https://perma.cc/48FB-WAZJ',
    'https://perma.cc/W9KK-8GVX?type=image',
    'https://perma.cc/7U2U-HDPK?type=image',
    'https://perma.cc/XZU5-WWDL?type=image',
    'https://perma.cc/D5QJ-WEVA',
    'https://archive.ph/Wz2hu',
    'https://archive.is/4oKus',
    'https://archive.ph/jVhsY',
    'https://archive.is/Wz2hu',
    'https://archive.vn/tqGEw',
    'https://archive.ph/TUnme',
    'https://archive.ph/TTEli'
]

fc_articles_to_exclude = [
    'https://www.factcheck.org/2020/03/factchecking-trumps-coronavirus-address/',
    'https://www.snopes.com/fact-check/covid-vaccine-funded-by-trump/',
    'https://factcheck.afp.com/trump-biden-trade-claims-coronavirus-response-first-debate',
    'https://www.snopes.com/fact-check/typical-year-covid-deaths/',
    'https://www.factcheck.org/2020/03/trumps-misplaced-blame-on-obama-for-coronavirus-tests/',
    'https://www.factcheck.org/2020/03/the-facts-on-trumps-travel-restrictions/',
    'https://www.snopes.com/fact-check/listen-to-scientists/',
    'https://www.factcheck.org/2021/07/scicheck-meme-trumpets-falsehood-about-delta-variant/',
    'https://www.factcheck.org/2021/07/factchecking-bidens-cnn-town-hall-3/',
    'https://www.factcheck.org/2021/07/scicheck-covid-19-surges-among-unvaccinated-in-florida-contrary-to-baseless-claims/',
    'https://www.politifact.com/factchecks/2021/oct/05/ron-johnson/johnson-incorrectly-claims-there-are-no-approved-c/',
    'https://www.factcheck.org/2021/10/scicheck-migrants-not-responsible-for-latest-covid-19-surge/',
    'https://www.factcheck.org/2020/06/trump-touts-strong-jobs-report-flubs-some-facts/',
    'https://www.snopes.com/fact-check/pregnant-women-covid-19-vaccine/',
    'https://healthfeedback.org/claimreview/joe-rogan-interview-with-peter-mccullough-contains-multiple-false-and-unsubstantiated-claims-about-the-covid-19-pandemic-and-vaccines/',
    'https://www.politifact.com/factchecks/2021/jul/22/joe-biden/biden-exaggerates-efficacy-covid-19-vaccines/',
    'https://www.snopes.com/fact-check/cdc-recall-pcr-covid-19-tests-failed-review/',
    'https://www.snopes.com/fact-check/lemons-coronavirus/',
    'https://www.factcheck.org/2021/08/scicheck-chiropractor-again-peddles-false-misleading-covid19-claims/'
]