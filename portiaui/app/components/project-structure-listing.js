import Ember from 'ember';
import {computedCanAddSpider} from '../services/dispatcher';

export default Ember.Component.extend({
    ajax: Ember.inject.service(),
    browser: Ember.inject.service(),
    dispatcher: Ember.inject.service(),
    notificationManager: Ember.inject.service(),
    routing: Ember.inject.service('-routing'),
    savingNotification: Ember.inject.service(),
    uiState: Ember.inject.service(),

    tagName: '',

    canAddSpider: computedCanAddSpider(),
    currentSpider: Ember.computed.readOnly('uiState.models.spider'),
    currentSchema: Ember.computed.readOnly('uiState.models.schema'),

    addSpiderTooltipText: Ember.computed('canAddSpider', {
        get() {
            if (this.get('canAddSpider')) {
                return 'Create a new Spider';
            } else {
                return 'You must visit a website before you can create a Spider';
            }
        }
    }),

    actions: {
        addSchema() {
            this.get('dispatcher').addSchema(this.get('project'), /* redirect = */true);
        },

        removeSchema(schema) {
            this.get('dispatcher').removeSchema(schema);
        },

        saveSchema(schema) {
            schema.save();
        },

        addSpider() {
            this.get('dispatcher').addSpider(this.get('project'), /* redirect = */true);
        },

        removeSpider(spider) {
            this.get('dispatcher').removeSpider(spider);
        },

        validateSpiderName(name) {
            const nm = this.get('notificationManager');
            if(!/^[a-zA-Z0-9][a-zA-Z0-9_\.-]*$/.test(name)) {
                nm.showWarningNotification(`Invalid spider name.
                    Only letters, numbers, underscores, dashes and dots are allowed.`);
                return false;
            }
            return true;
        },

        saveSpiderName(spider) {
            const nm = this.get('notificationManager');
            const url = `api/projects/${spider.get('project.id')}/` +
                        `spiders/${spider.get('id')}/rename`;
            const data = {
                dataType: 'json',
                contentType: 'application/json; charset=UTF-8',
                data: JSON.stringify({name: spider.get('name')})
            };

            const changeSpiderName = this.get('dispatcher').changeSpiderName(spider);
            const saving = this.get('savingNotification');

            saving.start();
            this.get('ajax').post(url, data)
                .then(changeSpiderName)
                .catch(() => {
                    nm.showErrorNotification(`Renaming the spider '${spider.get('id')}' failed.`);
                    spider.set('name', spider.get('id'));
                }).finally(() => saving.end());
        }
    }
});
