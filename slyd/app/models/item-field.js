import SimpleModel from './simple-model';

export default SimpleModel.extend({
    serializedProperties: ['name', 'type', 'is_required', 'weight', 'required', 'vary'],
    type: 'text',
    is_required: false,
    weight: 1.0,
    required: false,
    vary: false,
});
