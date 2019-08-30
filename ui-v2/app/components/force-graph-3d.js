import Component from '@ember/component';
import { get } from '@ember/object';
import ForceGraph3d from '3d-force-graph';

export default Component.extend({
  distances: null,
  node: null,
  didInsertElement: function() {
    const data = {};
    const node = get(this, 'node');
    const distances = get(this, 'distances');
    data.nodes = [
      {
        id: node,
        name: node,
        val: 0,
      },
    ].concat(
      distances.map(function(item) {
        return {
          id: item.node,
          name: item.node,
          val: item.distance,
        };
      })
    );
    data.links = distances.map(function(item) {
      return {
        source: node,
        target: item.node,
      };
    });
    ForceGraph3d()(this.element).graphData(data);
    console.log(get(this, 'distances'));
  },
});
